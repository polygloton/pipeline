(ns ^{:doc "Create and validate control messages"}
  pipeline.control
  (:require [clojure.core.async :as async]
            [pipeline.protocols :as prots]
            [pipeline.utils.schema :as local-schema]
            [schema.core :as schema]))

(def InputChan local-schema/Chan)

(def OutputChan local-schema/Chan)

(def KillSwitch
  (schema/both (schema/protocol prots/KillSwitch)
               (schema/protocol prots/Listener)))

(def Context schema/Any)

(def ControlMessage
  [(schema/one InputChan "input-chan")
   (schema/one OutputChan "output-chan")
   (schema/one KillSwitch "kill-switch")
   (schema/one Context "context")])

(schema/defn send-message-vec :- schema/Bool
  [control-chan :- local-schema/Chan
   message :- ControlMessage]
  (async/>!! control-chan message))

(defn send-message
  [control-chan
   & {:keys [input-chan output-chan kill-switch context]}]
  (send-message-vec control-chan
                    [input-chan output-chan kill-switch context]))
