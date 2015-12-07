(ns ^{:doc "Functions that extend clojure.core.async"}
    pipeline.utils.async
  (:require [clojure.core.async :as async]))

(defn channel? [inst]
  (satisfies? clojure.core.async.impl.protocols/Channel inst))
