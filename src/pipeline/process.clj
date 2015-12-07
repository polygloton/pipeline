(ns ^{:doc "Creates a CSP style process for composing data-processing pipelines

            The process is backed my multiple internal threads (green
            and possibly real).  Work is implemented using an instance of the
            PipelineImpl protocol.  The data-processing pipeline is generally
            instantiated once and is reused for multiple ad hoc tasks, which may
            be run concurrently.  Each task has a distinct input/output channel
            pair that are used by the pipeline process when handling messages.
            The channel pairing exists only as long as the input-channel remains
            open.  Multiple processes may be chained together (one's output
            channel being another's input channel) to create the data-processing
            pipeline.  Closing the first input channel cascades closure through
            the pipeline until the last output channel in the chain is closed.
            This is how termination detection is implemented.  An instance of
            the KillSwitch protocol is used to determine if there were any
            errors during processing and to quickly kill the ad hoc chain when
            there are errors.  Input/output channel pairings are established by
            sending control messages over a control channel.

            Inspired by clojure.core.async/pipeline, but with many differences."}
  pipeline.process
  (:require [clojure.core.async :as async]
            [pipeline.process.listener :as listener]
            [pipeline.process.worker :as worker]
            [pipeline.utils.schema :as local-schema]
            [schema.core :as schema]))

(defn- broadcast-control-messages
  "To support internal concurrency, pipeline.process is implemented
  with distinct listener and worker threads.  Each listener thread
  needs a unique internal control channel as well as unique output
  channels.  Internal control and output channels are created here and
  a green thread is created that intercepts control messages and sends
  modified control messages on the internal control channels.  Control
  messages are modified to use the internal output channels."
  [n external-control-chan]
  (let [internal-control-chans (repeatedly n #(async/chan 1))]
    (async/go-loop []
      (if-let [[input-chan output-chan kill-switch context]
               (async/<! external-control-chan)]
        (let [my-output-chans (repeatedly n async/chan)]
          (async/pipe (async/merge (vec my-output-chans)) output-chan)
          (dotimes [i n]
            (async/>! (nth internal-control-chans i) [input-chan
                                                      (nth my-output-chans i)
                                                      kill-switch
                                                      context]))
          (recur))
        (doseq [c internal-control-chans]
          (async/close! c))))
    internal-control-chans))



(schema/defn create :- (schema/eq nil)
  "Create a pipeline.process (a link in the data-processing chain)

     Takes:
     - concurrency number (> n 0)
     - Control channel to listen on
     - Fn that takes a context map (provided in control messages) and returns
       an instance of the PipelineImpl protocol (eg a factory)
     - mode keyword, either :blocking (for real threads) or :compute (for green
       threads).

     Returns: nil"
  [n :- local-schema/PosInt
   external-control-chan :- local-schema/Chan
   pimpl-factory :- (schema/pred fn?)
   pipeline-mode :- (schema/enum :blocking :compute)]
  (let [internal-control-chans
        (broadcast-control-messages n external-control-chan)

        jobs-chan
        (async/chan n)]
    (doseq [control-chan internal-control-chans]
      (worker/work pipeline-mode jobs-chan)
      (listener/listen control-chan jobs-chan pimpl-factory))))
