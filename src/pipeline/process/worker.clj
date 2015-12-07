(ns ^{:doc "The worker takes messages from the jobs channel and handles them
            on either a green thread or a real thread (configurable)."}
  pipeline.process.worker
  (:require [clojure.core.async :as async]
            [pipeline.protocols :as prots]
            [pipeline.process.messages :as messages]
            [pipeline.process.protocols :as process-prots]
            [pipeline.utils.schema :as local-schema]
            [schema.core :as schema]))

(def ^:private put-timeout-ms 10000)

(defn- handle-closed-out-chan [kill-switch chan item]
  (prots/kill! kill-switch
               {:message "Output channel was already closed"
                :chan chan
                :item item}))

(defn- call-handler-method [pimpl message state]
  (if (= message messages/none)
    (prots/finish pimpl (= state messages/ok))
    (prots/handle pimpl message)))

(defmacro ^:private handle-a-message
  [mode pipeline-impl message state]
  (let [alts-fn (case mode :blocking 'async/alts!! :compute 'async/alts!)]
    `(loop [items# (call-handler-method ~pipeline-impl ~message ~state)
            out-chan# (process-prots/out-chan ~pipeline-impl)]
       (let [next-item# (first items#)
             rest-items# (rest items#)]
         (cond
           (empty? items#)
           out-chan#

           (nil? next-item#)
           (recur rest-items# out-chan#)

           (async/offer! out-chan# next-item#)
           (recur rest-items# out-chan#)

           :else
           (let [result-chan#
                 (loop []
                   (let [[put-result# ignored#] (~alts-fn [[out-chan# next-item#]
                                                           (async/timeout put-timeout-ms)]
                                                          :priority true)]
                     (cond
                       (true? put-result#)
                       out-chan#

                       (false? put-result#)
                       (do (handle-closed-out-chan ~pipeline-impl next-item#)
                           (reduced out-chan#))

                       (prots/killed? ~pipeline-impl)
                       (reduced out-chan#)

                       :else ;; put timed out
                       (recur))))]
             (if (= result-chan# out-chan#)
               (recur rest-items# out-chan#)
               result-chan#)))))))

(defmacro ^:private handle-a-job [mode job]
  (let [put-fn (case mode :blocking 'async/>!! :compute 'async/>!)]
    `(let [[promise-chan# pipeline-impl# message# state#] ~job]
       (~put-fn promise-chan#
        (try
          (handle-a-message ~mode pipeline-impl# message# state#)
          (catch Throwable t# t#)))
       (async/close! promise-chan#))))

(schema/defn work :- local-schema/Chan
  "Start a worker process (on either a green or real thread)

   Takes:
   - Mode keyword indicating green or real
   - Jobs channel

   Returns: Go-block results channel

   The worker takes jobs off of the jobs channel and uses the
   PipelineImpl methods to handle inputs.  Successful results are put
   on the output-channel.  Exceptions are caught and passed back to
   the listener on the job's promise channel.  If putting on the
   output-channel is blocking, the kill-switch is periodically checked
   to see if there was an error on another thread."
  [mode :- (schema/enum :blocking :compute)
   jobs-chan :- local-schema/Chan]
  (case mode
      :blocking (async/thread
                  (when-let [job (async/<!! jobs-chan)]
                    (handle-a-job :blocking job)
                    (recur)))
      :compute (async/go-loop []
                 (when-let [job (async/<! jobs-chan)]
                   (handle-a-job :compute job)
                   (recur)))))
