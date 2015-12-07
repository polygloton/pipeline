(ns ^{:doc "Protocols that are used by the process internally
            Not intended for external use."}
    pipeline.process.protocols
  (:require [clojure.core.async :as async]
            [pipeline.protocols :as prots]))

(defprotocol PipelineTaskState
  "State tracked by the listener that is associated with an
  input-channel that it is listening to"

  (in-chan [self])
  (out-chan [self])
  (out-chan? [self chan])
  (close-out-chan! [self])
  (kill-chan [self])
  (kill-chan? [self chan]))

;; Implementation of multiple protocols, existing for the purpose of
;; tracking the state of a listener task and delegating actions to
;; wrapped instances.  Used to handle messages.
(defrecord PipelineTaskImpl [pimpl kill-switch in-chan kill-chan out-chan]

  PipelineTaskState
  (in-chan [_]
    in-chan)
  (out-chan [_]
    out-chan)
  (out-chan? [_ chan]
    (= out-chan chan))
  (close-out-chan! [_]
    (async/close! out-chan))
  (kill-chan [_]
    kill-chan)
  (kill-chan? [_ chan]
    (= kill-chan chan))

  prots/PipelineImpl
  (handle [_ input-message]
    (prots/handle pimpl input-message))
  (finish [_ completed?]
    (prots/finish pimpl completed?))

  prots/KillSwitch
  (killed? [_]
    (prots/killed? kill-switch))
  (kill! [_ details-m]
    (prots/kill! kill-switch details-m))
  (kill-exception! [_ exception]
    (prots/kill-exception! kill-switch exception)))
