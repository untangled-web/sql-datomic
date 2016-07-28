(ns sql-datomic.delete-command
  (:require [sql-datomic.datomic :as dat]
            [sql-datomic.util :as util :refer [squawk]]
            [datomic.api :as d]
            [clojure.pprint :as pp]))

(defn -run-harness [{:keys [conn db ir options ids]}]
  (let [{:keys [debug pretend silent]} options
        entities (->> (util/get-entities-by-eids db ids)
                      dat/keep-genuine-entities)]
    (when debug (squawk "Entities Targeted for Delete"))
    ;; Always give indication of what will be deleted.
    (when-not silent
      (println (if (seq entities) ids "None")))
    (when debug (util/-debug-display-entities entities))

    (if-not (seq entities)
      {:ids ids
       :entities entities}

      ;; else
      (let [tx-data (dat/delete-eids->tx-data ids)]
        (when debug (squawk "Transaction" tx-data))
        (if pretend
          (do
            (println "Halting transaction due to pretend mode ON")
            {:ids ids
             :entities entities
             :pretend pretend
             :tx-data tx-data})

          ;; else
          (let [result @(d/transact conn tx-data)]
            (when-not silent
              (println)
              (println result))
            (when debug
              (squawk "Entities after Transaction")
              (util/-debug-display-entities-by-ids (:db-after result) ids))
            {:ids ids
             :entities entities
             :tx-data tx-data
             :result result}))))))

(defn -run-db-id-delete
  [conn db ir opts]
  (let [ids (dat/db-id-clause-ir->eids ir)]
    (-run-harness {:conn conn
                   :db db
                   :ir ir
                   :options opts
                   :ids ids})))

(defn -run-normal-delete
  [conn db {:keys [where table] :as ir} {:keys [debug] :as opts}]
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
        (throw
         (IllegalArgumentException.
          "\n!!! Missing delete target. Necessary with joins. Aborting delete !!!\n"))))))

(defn run-delete
  ([conn db ir] (run-delete conn db ir {}))
  ([conn db {:keys [where] :as ir} opts]
   {:pre [(= :delete (:type ir))]}
   (when (seq where)
     (if (dat/db-id-clause? where)
       (-run-db-id-delete conn db ir opts)
       (-run-normal-delete conn db ir opts)))))
