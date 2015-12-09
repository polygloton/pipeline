  (ns ^{:doc "The listener takes messages from input channels and handles
              them by creating jobs and submitting them on the jobs
              channel.  Also tracks state related to input-channels."}
    pipeline.process.listener
    (:require [clojure.core.async :as async]
              [clojure.core.match :refer [match]]
              [pipeline.process.messages :as messages]
              [pipeline.process.protocols :refer :all]
              [pipeline.protocols :as prots]
              [pipeline.utils.async :as local-async]
              [pipeline.utils.schema :as local-schema]
              [schema.core :as schema]))

(defn- submit-a-job
  "Asynchronously put a job message onto the jobs channel and wait for
  the result.  Returns a results channel."
  [jobs-chan pipeline-impl message state]
  (async/go
    (let [promise-chan (async/chan 1)]
      (async/>! jobs-chan [promise-chan pipeline-impl message state])
      (async/<! promise-chan))))

(schema/defn listen :- local-schema/Chan
  "Start a listener green thread

   Takes:
   - Internal control channel
   - Jobs channel
   - Factory fn that is called with a context to create a PipelineImpl
     instance

   Returns: Go-block results channel

   The listener thread maintains a vector of channels to listen on,
   starting with just the internal control channel.  When it gets
   control messages, it adds the input channel to its vector of
   channels and tracks state for that input channel.  It then listens
   for messages on an all of its input channels and the control
   channel. Messages are made into jobs which are put onto the jobs
   channel.  Workers pick up jobs and put results onto the
   output-channel, possibly catching and returning exceptions.  When
   exceptions are caught the kill-switch is triggered, errors are
   stored, and work is halted for the related input-channel.  Closing
   an input channel triggers closing the associate output channel, as
   well as forgetting the state."
  [control-input-chan :- local-schema/Chan
   jobs :- local-schema/Chan
   pimpl-factory-fn :- (schema/pred fn?)]
  (async/go-loop [input-chans [control-input-chan]
                  input-chan->pimpl {}]
    (if (= [] input-chans)
      (async/close! jobs)
      (match
       (async/alts! input-chans)

       [nil control-input-chan]
       (recur (filterv (partial not= control-input-chan) input-chans)
              input-chan->pimpl)

       [[input-chan output-chan kill-switch context]
        control-input-chan]
       (let [kill-chan (prots/tap kill-switch (async/chan (async/dropping-buffer 1)))
             pimpl (pimpl-factory-fn context)]
         (if (satisfies? prots/PipelineImpl pimpl)
           (recur (into input-chans [input-chan kill-chan])
                  (assoc input-chan->pimpl
                         input-chan (map->PipelineTaskImpl
                                     {:pimpl pimpl
                                      :kill-switch kill-switch
                                      :out-chan output-chan
                                      :kill-chan kill-chan
                                      :in-chan input-chan})
                         kill-chan input-chan))
           (do (prots/kill! kill-switch
                            {:message "Factory did not return a PipelineImpl"
                             :context context})
               (async/close! output-chan)
               (recur input-chans input-chan->pimpl))))

       [_ control-input-chan]
       (recur input-chans input-chan->pimpl)

       [message message-chan]
       (let [inchan-or-state
             (get input-chan->pimpl message-chan)

             [input-chan pipeline-task-impl]
             (if (local-async/channel? inchan-or-state)
               [inchan-or-state (get input-chan->pimpl inchan-or-state)]
               [message-chan inchan-or-state])

             killed?
             (kill-chan? pipeline-task-impl message-chan)

             [message some-message?]
             (cond
               (nil? message) [messages/none false]
               killed? [messages/none false]
               :else [message true])

             job-result
             (async/<! (submit-a-job jobs
                                     pipeline-task-impl
                                     message
                                     (if killed? messages/killed messages/ok)))]

         (cond
           (instance? Throwable job-result)
           (do (prots/kill-exception! pipeline-task-impl
                                      (ex-info "Job handler threw exception"
                                               {:input-message message
                                                :message-chan message-chan}
                                               job-result))
               (if some-message?
                 (async/<! (submit-a-job jobs pipeline-task-impl messages/none messages/exception))))

           (reduced? job-result)
           (do (prots/kill! pipeline-task-impl {:message "Job was aborted during processing"
                                                :input-message message
                                                :message-chan message-chan})
               (if some-message?
                 (async/<! (submit-a-job jobs pipeline-task-impl messages/none messages/killed)))))

         (if (and some-message?
                  (out-chan? pipeline-task-impl job-result))
           (recur input-chans input-chan->pimpl)
           (do (close-out-chan! pipeline-task-impl)
               (recur (filterv (complement
                                #(some (partial = %)
                                       [input-chan (kill-chan pipeline-task-impl)]))
                               input-chans)
                      (dissoc input-chan->pimpl
                              input-chan
                              kill-chan)))))))))
