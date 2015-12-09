# pipeline

A Clojure library for building data-processing chain pipelines

## Usage

Add this dependency to your leiningen project.clj

[polyglogon/pipeline "0.1.0"]

### Example code

Here is a contrived example that repeatedly reuses a pipeline

``` clojure
(ns pipeline-example.core
  (:require [clojure.core.async :as async]
            [clojure.pprint :refer [pprint]]
            [pipeline.control :as control]
            [pipeline.kill-switch :as kill-switch]
            [pipeline.protocols :refer [PipelineImpl]]
            [pipeline.process :as process]))

(defn- square [x]
  (* x x))

(defrecord Squarer [_]
  PipelineImpl
  (handle [_ input-message]
    [(square input-message)])
  (finish [_ _]
    []))

(defrecord Summer [accum]
  PipelineImpl
  (handle [_ input-message]
    (swap! accum + input-message)
    [])
  (finish [_ completed?]
    (if completed?
      [@accum])))

(defn summer-factory [ignored]
  (->Summer (atom 0)))

(defn run []
  (let [kill-switch (kill-switch/create)
        control-chan-1 (async/chan 1)
        control-chan-2 (async/chan 1)]
    (process/create 3
                    control-chan-1
                    ->Squarer
                    :compute)
    (process/create 1
                    control-chan-2
                    summer-factory
                    :compute)
    (doseq [i (range 100)
            :let [in-chan-1 (async/chan)
                  in-chan-2 (async/chan)
                  out-chan-2 (async/chan)]]
      (async/onto-chan in-chan-1 [1 2 3 4 5])
      (control/send-message control-chan-1
                            :input-chan in-chan-1
                            :output-chan in-chan-2
                            :kill-switch kill-switch
                            :context {:foo :bar})
      (control/send-message control-chan-2
                            :input-chan in-chan-2
                            :output-chan out-chan-2
                            :kill-switch kill-switch
                            :context {:spam :eggs})
      (pprint (async/<!! out-chan-2)))
    (map async/close! [control-chan-1 control-chan-2])))
```

There are more examples in the test code

### Data-processing pipeline

The data-processing [pipeline](https://en.wikipedia.org/wiki/Pipeline_%28computing%29)
"is a set of data processing elements connected in series, where the output of
one element is the input of the next one".  This library makes it easy  to create
the processing elements (called processes here) and to coordinate processing
tasks on the pipeline.

### Processes

A link in the chain.  TODO.

### Messages

TODO

### Kill-Switch

TODO

### Control messages

TODO

### Termination detection

TODO

## Differences with clojure.core.async/pipeline

This pipeline implementation is inspired by core.async/pipeline, but
there are a few differences.

* Uses instances of the PipelineImpl protocol instead of transducers.
* This pipeline does not not try to maintain output order, the
  core.async/pipeline does.
* Instances of PipelineImpl are reused for handling multiple messages,
  but keep in mind that messages will be spread across multiple
  instances if concurrency is greater than one.  The core.async/pipeline
  create a new xform instance for each message.
* The processes used in this pipeline are instantiated separately from
  the input/output channels.  This pipeline can know about multiple
  input/output channel pairs at the same time, and will work on tasks
  as long as control channel(s) are open.

## License

Copyright © 2014-2015 Staples, Inc.
Distributed under the MIT License
