(ns pipeline.kill-switch-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer :all]
            [pipeline.kill-switch :as kill-switch]
            [pipeline.protocols :as p]))

(deftest ^:unit test-create-atomic-kill-switch
  (testing "with no kills"
    (let [sw (kill-switch/create)]
      (is (false? (p/killed? sw)))
      (is (= [] (p/errors sw)))
      (is (nil? (p/first-error sw)))))

  (testing "with 1 kill"
    (with-redefs [kill-switch/now (constantly :mock)]
      (let [sw (kill-switch/create)]
        (p/kill! sw {:message "This is a test"})
        (is (true? (p/killed? sw)))
        (is (= [{:message "This is a test"
                 :timestamp :mock}]
               (p/errors sw)))
        (is (= {:message "This is a test"
                :timestamp :mock}
               (p/first-error sw))))))

  (testing "with 2 kills"
    (with-redefs [kill-switch/now (constantly :mock)]
      (let [sw (kill-switch/create)]
        (p/kill! sw {:message "This is a test"})
        (p/kill! sw {:message "This is another test"})
        (is (true? (p/killed? sw)))
        (is (= [{:message "This is a test"
                 :timestamp :mock}
                {:message "This is another test"
                 :timestamp :mock}]
               (p/errors sw)))
        (is (= {:message "This is a test"
                :timestamp :mock}
               (p/first-error sw))))))

  (testing "with 1 exception kill"
    (with-redefs [kill-switch/now (constantly :mock)]
      (let [sw (kill-switch/create)]
        (p/kill-exception! sw (ex-info "This is a test" {:message2 "BOOM!"}))
        (is (true? (p/killed? sw)))
        (is (= [{:message "This is a test",
                 :message2 "BOOM!",
                 :timestamp :mock,
                 :class "class clojure.lang.ExceptionInfo"}]
               (map #(select-keys % [:message :message2 :timestamp :class])
                    (p/errors sw))))
        (is (= {:message "This is a test",
                :message2 "BOOM!",
                :timestamp :mock,
                :class "class clojure.lang.ExceptionInfo"}
               (select-keys (p/first-error sw)
                            [:message :message2 :timestamp :class]))))))

  (testing "listner receives errors"
    (with-redefs [kill-switch/now (constantly :mock)]
      (let [sw (kill-switch/create)
            ch (async/chan 1)]
        (is (= ch (p/tap sw ch)))
        (p/kill! sw {:message "This is a test"})
        (is (= [{:message "This is a test"
                 :timestamp :mock} ch] (async/alts!! [ch (async/timeout 100)] :priority true)))
        (p/kill! sw {:message "This is another test"})
        (is (= [{:message "This is another test"
                 :timestamp :mock} ch] (async/alts!! [ch (async/timeout 100)] :priority true)))
        (p/close! sw)
        (is (= [nil ch] (async/alts!! [ch (async/timeout 100)] :priority true)))))))
