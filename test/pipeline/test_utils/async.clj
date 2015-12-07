(ns ^{:doc "Extensions to core.async used only in tests"}
  pipeline.test-utils.async
  (:require [clojure.core.async :as async]
            [clojure.core.async.impl.protocols :as async-proto]))

(defmacro go-try
  "A core.async/go block, with an implicit try...catch. Exceptions are
   returned (put onto the go block's result channel)."
  [& body]
  `(async/go
     (try
       ~@body
       (catch Throwable t#
         t#))))

(defn throw-err
  "Throw element if it is Throwable, otherwise return it"
  [element]
  (when (instance? Throwable element)
    (throw element))
  element)

(defn <??
  "Like core.async/<!! but throws if the message is Throwable"
  [ch]
  (throw-err (async/<!! ch)))

(defn take-all!
  "Take from a channel until it is closed, returning the accumulated
  output.  Throws RuntimeException if not completed within a timeout
  (default is 200ms)."
  [chan & {timeout-ms :timeout
           :or {timeout-ms 200}}]
  (let [results-chan (async/chan)]
    (async/go
      (loop [results []]
        (let [item (async/<! chan)]
          (if (nil? item)
            (async/>! results-chan results)
            (recur (conj results item))))))
    (let [[item from-chan]
          (async/alts!! [results-chan (async/timeout timeout-ms)] :priority true)]
      (if (not= from-chan results-chan)
        (throw (RuntimeException. (format "Timed out after %dms" timeout-ms)))
        item))))

(defn closed? [chan]
  (async-proto/closed? chan))
