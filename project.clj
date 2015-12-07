(defproject pipeline "0.1.0-SNAPSHOT"
  :description "Data-processing chain pipeline abstraction using clojure.core.async"
  :license {:name "MIT License"
            :url "http://mit-license.org/"}
  :dependencies [[org.clojure/clojure    "1.7.0"]
                 [org.clojure/core.async "0.2.371"]
                 [org.clojure/core.match "0.3.0-alpha4"]
                 [prismatic/schema       "0.4.3"]])
