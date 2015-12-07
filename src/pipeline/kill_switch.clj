(ns ^{:doc "Implement a kill switch to be used by pipeline.process

           The kill-switch is shared by all pipeline.process instances
           to halt all activities across the data-processing chain when an
           exception occurs.  The kill-switch should also be checked by code
           using a pipeline once the work is done (to determine if output can
           be considered valid)."}
  pipeline.kill-switch
  (:require [clojure.core.async :as async]
            [pipeline.protocols :refer :all])
  (:import java.sql.SQLException))

(def ^:private min-kill-messages 20)

(defn- exception->map
  "Build a map from an exception.  Copied out of staples-sparx/kits
  rev 67c48c5 instead of adding a lot of irrelevant deps.  MIT license."
  [^Throwable e]
  (merge
   {:class (str (class e))
    :message (.getMessage e)
    :stacktrace (mapv str (.getStackTrace e))}
   (when (.getCause e)
     {:cause (exception->map (.getCause e))})
   (if (instance? SQLException e)
     (if-let [ne (.getNextException ^SQLException e)]
       {:next-exception (exception->map ne)}))))

(defn- now
  "Current time in millis since the epoch
   Keep here for ease of mocking"
  []
  (System/currentTimeMillis))

(defrecord AtomicKillSwitch [state listener-chan listener-mult]

  KillSwitch
  (killed? [_]
    (boolean (seq @state)))
  (kill! [_ details-m]
    (let [details-m2 (merge details-m {:timestamp (now)})]
      (swap! state (fn [x] (conj x details-m2)))
      (async/>!! listener-chan details-m2)))
  (kill-exception! [switch exception]
    (kill! switch (merge (ex-data exception)
                         (exception->map exception))))

  ErrorRepo
  (errors [_] @state)
  (first-error [_] (first @state))

  Listener
  (tap [_ chan]
    (async/tap listener-mult chan))
  (close! [_]
    (async/close! listener-chan)))

(defn create []
  (map->AtomicKillSwitch
   (let [listener-chan (async/chan (async/dropping-buffer min-kill-messages))]
     {:state (atom [])
      :listener-chan listener-chan
      :listener-mult (async/mult listener-chan)})))
