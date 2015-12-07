(defproject polygloton/pipeline "0.1.0"
  :description "Data-processing pipeline abstraction using clojure.core.async"
  :license {:name "MIT License"
            :url "http://mit-license.org/"}
  :dependencies [[org.clojure/core.async "0.2.371"]
                 [org.clojure/core.match "0.3.0-alpha4"]
                 [prismatic/schema       "0.4.3"]]
  :profiles {:1.5 {:dependencies [[org.clojure/clojure "1.5.0"]]}
             :1.6 {:dependencies [[org.clojure/clojure "1.6.0"]]}
             :1.7 {:dependencies [[org.clojure/clojure "1.7.0"]]}})
