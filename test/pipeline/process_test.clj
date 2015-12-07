(ns ^{:doc "Workout the pipeline.process code"}
  pipeline.process-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer :all]
            [pipeline.kill-switch :as kill-switch]
            [pipeline.process :as process]
            [pipeline.process.worker :as worker]
            [pipeline.protocols :refer [PipelineImpl killed? errors]]
            [pipeline.test-utils :as test-utils]
            [pipeline.test-utils.async :as async-utils]
            [schema.core :as schema]))

(use-fixtures :once (fn [t]
                      (schema/with-fn-validation (t))))

(defn- square [x]
  (* x x))

(defrecord Squarer [finished?]
  PipelineImpl
  (handle [_ input-message]
    [(square input-message)])
  (finish [_ completed?]
    (reset! finished? completed?)
    []))

(defn squarer-factory [finished?]
  (fn [ignored-context]
    (->Squarer finished?)))

(defrecord Sleeper [ignored-context]
  PipelineImpl
  (handle [_ input-message]
    (Thread/sleep 20)
    [input-message])
  (finish [_ _]
    []))

(defrecord Puker [finished? counter]
  PipelineImpl
  (handle [_ input-message]
    (if (>= @counter 2)
      (throw (RuntimeException. "Blah!"))
      (swap! counter inc))
    [input-message])
  (finish [_ completed?]
    (reset! finished? completed?)
    []))

(defn puker-factory [finished?]
  (fn [ignored-context]
    (->Puker finished? (atom 0))))

(defrecord FinSquarer [finished? accum]
  PipelineImpl
  (handle [_ input-message]
    (swap! accum conj (square input-message))
    [])
  (finish [_ completed?]
    (reset! finished? completed?)
    (if completed?
      @accum
      [])))

(defn fin-squarer-factory [finished?]
  (fn [ignored-context]
    (->FinSquarer finished? (atom []))))

(defrecord Duplicator [finished?]
  PipelineImpl
  (handle [_ input-message]
    ;; Notice that nil values gets discarded by the pipeline (it is
    ;; not put on the output channel because that would close the
    ;; channel).
    [input-message input-message nil input-message])
  (finish [_ completed?]
    (reset! finished? completed?)
    []))

(defn duplicator-factory [finished?]
  (fn [ignored-context]
    (->Duplicator finished?)))

(deftest ^:unit test-parellel-compute-pipelines
  (let [kill-switch (kill-switch/create)

        ;; pipeline: squarer-1
        control-chan-1 (async/chan 1)
        squarer-1-finished? (atom ::unknown)
        in-chan-1 (async/chan)
        out-chan-1 (async/chan)
        _ (async/>!! control-chan-1 [in-chan-1 out-chan-1 kill-switch {:p 1}])
        _ (process/create 3
                          control-chan-1
                          (squarer-factory squarer-1-finished?)
                          :compute)
        squarer-1-results (async-utils/go-try
                           (async-utils/take-all! out-chan-1))

        ;; pipeline: squarer-2
        control-chan-2 (async/chan 1)
        squarer-2-finished? (atom ::unknown)
        in-chan-2 (async/chan)
        out-chan-2 (async/chan)
        _ (async/>!! control-chan-2 [in-chan-2 out-chan-2 kill-switch {:p 2}])
        _ (process/create 3
                          control-chan-2
                          (squarer-factory squarer-2-finished?)
                          :compute)
        squarer-2-results (async-utils/go-try
                           (async-utils/take-all! out-chan-2))]

    ;; Send input asynchronously
    (async/onto-chan in-chan-1 [0 1 2 3 4 5 6 7 8 9])
    (async/onto-chan in-chan-2 [10 11 12 13 14 15 16 17 18 19])

    (testing "Correct outut for squarer-1"
      (is (= (set (async-utils/<?? squarer-1-results))
             #{0 1 4 9 16 25 36 49 64 81})))

    (testing "Correct output for squarer-2"
      (is (= (set (async-utils/<?? squarer-2-results))
             #{100 121 144 169 196 225 256 289 324 361})))

    ;; close control channels after all work is done
    (doseq [ch [control-chan-1 control-chan-2]]
      (async/close! ch))

    (testing "Squarer-1 properly cleaned up"
      (is (true? @squarer-1-finished?))
      (is (async-utils/closed? control-chan-1))
      (is (async-utils/closed? in-chan-1))
      (is (async-utils/closed? out-chan-1)))

    (testing "Squarer-2 properly cleaned up"
      (is (true? @squarer-2-finished?))
      (is (async-utils/closed? control-chan-2))
      (is (async-utils/closed? in-chan-2))
      (is (async-utils/closed? out-chan-2)))

    (testing "The kill switch was not triggered"
      (is (false? (killed? kill-switch))))))

(deftest ^:unit test-chained-compute&blocking-pipelines
  (let [kill-switch (kill-switch/create)

        ;; pipeline: squarer-1
        control-chan-1 (async/chan 1)
        squarer-1-finished? (atom ::unknown)
        in-chan-1 (async/chan)
        out-chan-1 (async/chan)
        _ (async/>!! control-chan-1 [in-chan-1 out-chan-1 kill-switch {:p 1}])
        _ (process/create 3
                          control-chan-1
                          (fin-squarer-factory squarer-1-finished?)
                          :compute)

        ;; pipeline: sleeper
        control-chan-2 (async/chan 1)
        in-chan-2 out-chan-1
        out-chan-2 (async/chan)
        _ (async/>!! control-chan-2 [in-chan-2 out-chan-2 kill-switch {:p 2}])
        _ (process/create 10
                          control-chan-2
                          ->Sleeper
                          :blocking)

        ;; pipeline: squarer-2
        control-chan-3 (async/chan 1)
        squarer-2-finished? (atom ::unknown)
        in-chan-3 out-chan-2
        out-chan-3 (async/chan)
        _ (async/>!! control-chan-3 [in-chan-3 out-chan-3 kill-switch {:p 3}])
        _ (process/create 3
                          control-chan-3
                          (squarer-factory squarer-2-finished?)
                          :compute)

        ;; pipeline: duplicator
        control-chan-4 (async/chan 1)
        duplicator-finished? (atom ::unknown)
        in-chan-4 out-chan-3
        out-chan-4 (async/chan)
        _ (async/>!! control-chan-4 [in-chan-4 out-chan-4 kill-switch {:p 4}])
        _ (process/create 3
                          control-chan-4
                          (duplicator-factory duplicator-finished?)
                          :compute)

        chain-results (async-utils/go-try
                       (async-utils/take-all! out-chan-4 :timeout 500))]

    ;; send input asynchronously
    (async/onto-chan in-chan-1 [0 1 2 3 4 5 6 7 8 9])

    (testing "Correct output for the pipeline chain"
      (let [results (async-utils/<?? chain-results)]
        (is (= (set results)
               #{0 1 16 81 256 625 1296 2401 4096 6561}))
        (is (= 30 (count results)))))

    ;; close control channels after all work is done
    (doseq [ch [control-chan-1 control-chan-2 control-chan-3 control-chan-4]]
      (async/close! ch))

    (testing "Squarer-1 properly cleaned up"
      (is (true? @squarer-1-finished?))
      (is (async-utils/closed? control-chan-1))
      (is (async-utils/closed? in-chan-1))
      (is (async-utils/closed? out-chan-1)))

    (testing "Sleeper properly cleaned up"
      (is (async-utils/closed? control-chan-2))
      (is (async-utils/closed? in-chan-2))
      (is (async-utils/closed? out-chan-2)))

    (testing "Squarer-2 properly cleaned up"
      (is (true? @squarer-2-finished?))
      (is (async-utils/closed? control-chan-3))
      (is (async-utils/closed? in-chan-3))
      (is (async-utils/closed? out-chan-3)))

    (testing "Duplicator properly cleaned up "
      (is (true? @duplicator-finished?))
      (is (async-utils/closed? control-chan-4))
      (is (async-utils/closed? in-chan-4))
      (is (async-utils/closed? out-chan-4)))

    (testing "The kill switch was not triggered"
      (is (false? (killed? kill-switch))))))

(deftest ^:unit test-pipeline-gets-killed-on-exception
  (with-redefs [worker/put-timeout-ms 5]
    (let [kill-switch (kill-switch/create)

          ;; pipeline: squarer-1
          control-chan-1 (async/chan 1)
          squarer-1-finished? (atom ::unknown)
          in-chan-1 (async/chan)
          out-chan-1 (async/chan)
          _ (async/>!! control-chan-1 [in-chan-1 out-chan-1 kill-switch {:p 1}])
          _ (process/create 3
                            control-chan-1
                            (squarer-factory squarer-1-finished?)
                            :compute)

          ;; pipeline: puker
          control-chan-2 (async/chan 1)
          puker-finished? (atom ::unknown)
          in-chan-2 out-chan-1
          out-chan-2 (async/chan)
          _ (async/>!! control-chan-2 [in-chan-2 out-chan-2 kill-switch {:p 2}])
          _ (process/create 1
                            control-chan-2
                            (puker-factory puker-finished?)
                            :compute)

          ;; pipeline: squarer-2
          control-chan-3 (async/chan 1)
          squarer-2-finished? (atom ::unknown)
          in-chan-3 out-chan-2
          out-chan-3 (async/chan)
          _ (async/>!! control-chan-3 [in-chan-3 out-chan-3 kill-switch {:p 3}])
          _ (process/create 3
                            control-chan-3
                            (squarer-factory squarer-2-finished?)
                            :compute)]

      ;; Send input asynchronously
      (async/onto-chan in-chan-1 [0 1 2 3 4 5 6 7 8 9])

      ;; Consume (and discard) output; normally you would test if the kill-switch
      ;; was triggered to decide if output is valid.  Output should be consumed
      ;; so that all channels will be closed and pipelines can forget them.
      (async-utils/take-all! out-chan-3 :timeout 500)

      ;; Close control channels after all work is done; This allows the
      ;; pipelines to terminate cleanly.
      (doseq [ch [control-chan-1 control-chan-2 control-chan-3]]
        (async/close! ch))

      (testing "Squarer-1 properly cleaned up"
        ;; Notice that there is no assertion that in-chan-1 is closed.
        ;; It would likely not get closed after an exception; it gets
        ;; abandoned.  Once it goes out of scope it will be GC'ed,
        ;; along with the green thread that is parked, waiting to put
        ;; data onto it.
        (is (false? (test-utils/attempt-until #(deref squarer-1-finished?)
                                              false?
                                              :ms-per-loop 100
                                              :timeout 1000)))
        (is (async-utils/closed? control-chan-1))
        ;; The output channel may have items on it.  Normally we don't care
        ;; because it will get forgotten and GC'ed.  But we want to assert
        ;; that it gets closed in this test.  It won't appear to be closed
        ;; unless it is empty.  Therefore we must drain it here.
        ;;
        ;; 2015-12-04 polygloton: Commenting out this test because it
        ;;                        intermittently times out. I would like to get
        ;;                        it to pass.
        ;;(ua/take-all! out-chan-1 :timeout 500)
        ;;(is (ua/closed? out-chan-1))
        )

      (testing "Puker properly cleaned up"
        (is (false? @puker-finished?))
        (is (async-utils/closed? control-chan-2))
        ;;(is (ua/closed? in-chan-2))
        (is (async-utils/closed? out-chan-2)))

      (testing "Squarer-2 properly cleaned up"
        ;; Can't know if squarer-2-finished? will be true or false.
        ;; It will be a race on in-chan or kill-chan closing.
        ;; Either is fine as long as caller checks the kill-switch.
        (is (not= ::unknown @squarer-2-finished?))
        (is (async-utils/closed? control-chan-3))
        (is (async-utils/closed? in-chan-3))
        (is (async-utils/closed? out-chan-3)))

      (testing "The kill switch was triggered"
        (is (true? (killed? kill-switch)))))))
