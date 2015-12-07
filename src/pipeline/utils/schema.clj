(ns ^{:doc "Custom schemas"}
    pipeline.utils.schema
  (:require [schema.core :as schema])
  (:import clojure.core.async.impl.protocols.Channel))

(def PosInt (schema/both schema/Int (schema/pred pos? "pos?")))

(def Chan Channel)
