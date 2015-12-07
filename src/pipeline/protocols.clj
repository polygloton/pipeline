(ns ^{:doc "Protocols used by the pipeline"}
    pipeline.protocols)

(defprotocol KillSwitch
  (killed? [switch])
  (kill! [switch details-m])
  (kill-exception! [switch exception]))

(defprotocol ErrorRepo
  (errors [switch])
  (first-error [switch]))

(defprotocol Listener
  (tap [switch chan])
  (close! [switch]))

(defprotocol PipelineImpl
  (handle [self input-message])
  (finish [self completed?]))
