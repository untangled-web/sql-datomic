(ns sql-datomic.update-command
  (:require [sql-datomic.datomic :as dat]
            [sql-datomic.util :as util
             :refer [squawk -debug-display-entities]]
            [datomic.api :as d]
            [clojure.pprint :as pp]))

(defn -run-harness [{:keys [conn db ir options ids]}]
  (let [{:keys [debug pretend silent]} options
        entities (->> (util/get-entities-by-eids db ids)
                      dat/keep-genuine-entities)]
    (when debug (squawk "Entities Targeted for Update"))
    (when-not silent
      (println (if (seq entities) ids "None")))
    (when debug (-debug-display-entities entities))

    (if-not (seq entities)
      {:ids ids
       :before entities
       :pretend pretend}

      (let [base-tx (dat/update-ir->base-tx-data ir)
            tx-data (dat/stitch-tx-data base-tx ids)]
        (when debug (squawk "Transaction" tx-data))
        (if pretend
          (do
            (println "Halting transaction due to pretend mode ON")
            {:ids ids
             :before entities
             :tx-data tx-data
             :pretend pretend})

          (let [result @(d/transact conn tx-data)]
            (when-not silent
              (println)
              (println result))
            (let [entities' (util/get-entities-by-eids (:db-after result) ids)]
              (when debug
                (squawk "Entities after Transaction")
                (-debug-display-entities entities'))
              {:ids ids
               :tx-data tx-data
               :before entities
               :after entities'
               :result result})))))))

(defn -run-db-id-update
  [conn db ir opts]
  (let [ids (dat/db-id-clause-ir->eids ir)]
    (-run-harness {:conn conn
                   :db db
                   :ir ir
                   :options opts
                   :ids ids})))

(defn -run-normal-update
  [conn db {:keys [where table assign-pairs] :as ir}
           {:keys [debug] :as opts}]
  (let [query (dat/where->datomic-q db where)]
    (when debug
      (squawk "Datomic Rules" dat/rules)
      (squawk "Datomic Query" query))

    (let [results (d/q query db dat/rules)]
      (when debug (squawk "Raw Results" results))
      (cond
        (or (->> results seq not)
            (->> results first count (= 1)))
        (let [ids (mapcat identity results)]
          (-run-harness {:conn conn
                         :db db
                         :ir ir
                         :options opts
                         :ids ids}))

        ;; Assertion:  results is non-empty
        ;; Assertion:  tuples in results have arity > 1
        (seq table)
        (let [i (util/infer-entity-index ir query)
              ids (map (fn [row] (nth row i)) results)]
          (when debug
            (binding [*out* *err*]
              (printf "Narrowing entity vars to index: %d\n" i)
              (prn [:ids ids])))
          (-run-harness {:conn conn
                         :db db
                         :ir ir
                         :options opts
                         :ids ids}))

        ;; Assertion:  no given table
        :else
        (let [table-names (->> assign-pairs
                               util/summarize-ir-columns-by-table
                               keys)]
          (if (= 1 (count table-names))
            (let [t (first table-names)
                  i (util/infer-entity-index ir query t)
                  ids (map (fn [row] (nth row i)) results)]
              (when debug
                (binding [*out* *err*]
                  (printf "Narrowing entity vars to index: %d\n" i)
                  (prn [:ids ids])))
              (-run-harness {:conn conn
                             :db db
                             :ir ir
                             :options opts
                             :ids ids}))
            ;; else
            (throw
             (IllegalArgumentException.
              "\n!!! Missing update target. Necessary with joins. Aborting update !!!\n"))))))))

(defn run-update
  ([conn db ir] (run-update conn db ir {}))
  ([conn db {:keys [where] :as ir} opts]
   {:pre [(= :update (:type ir))]}
   (when (seq where)
     (if (dat/db-id-clause? where)
       (-run-db-id-update conn db ir opts)
       (-run-normal-update conn db ir opts)))))
