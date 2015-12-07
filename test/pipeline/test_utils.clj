(ns pipeline.test-utils)

(defn attempt-until
  "Copied from staples-sparx/kits rev 67c48c5 here to avoid adding
  more deps.  License MIT."
  [f done?-fn & {:keys [ms-per-loop timeout]
                 :or {ms-per-loop 1000
                      timeout 10000}}]
  (loop [elapsed (long 0)
         result (f)]
    (if (or (done?-fn result)
            (>= elapsed timeout))
      result
      (do
        (Thread/sleep ms-per-loop)
        (recur (long (+ elapsed ms-per-loop)) (f))))))
