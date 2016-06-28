(ns sql-datomic.update-command
  (:require [sql-datomic.datomic :as dat]
            [sql-datomic.util :refer [squawk -debug-display-entities]]
            [datomic.api :as d]
            [clojure.pprint :as pp]))

(defn -run-harness [{:keys [conn db ir options ids]}]
  (let [{:keys [where]} ir
        {:keys [debug pretend]} options
        entities (->> (dat/get-entities-by-eids db ids)
                      dat/keep-genuine-entities)]
    (when debug (squawk "Entities Targeted for Update"))
    (println (if (seq entities) ids "None"))
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
            (println)
            (println result)
            (let [entities' (dat/get-entities-by-eids (d/db conn) ids)]
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
  [conn db {:keys [where] :as ir} {:keys [debug] :as opts}]
  (let [query (dat/where->datomic-q db where)]
    (when debug
      (squawk "Datomic Rules" dat/rules)
      (squawk "Datomic Query" query))
    (let [results (d/q query db dat/rules)]
      (when debug (squawk "Raw Results" results))
      ;; FIXME: this is *not* the case, transacts on *all*
      ;;        eids participating in joins.
      ;; UPDATE pertains to only one "table" (i.e., no
      ;; joins), so this flattened list of ids is okay.
      (let [ids (mapcat identity results)]
        (-run-harness {:conn conn
                       :db db
                       :ir ir
                       :options opts
                       :ids ids})))))

(defn run-update
  ([conn db ir] (run-update conn db ir {}))
  ([conn db {:keys [where] :as ir} {:keys [debug] :as opts}]
   {:pre [(= :update (:type ir))]}
   (when (seq where)
     (if (dat/db-id-clause? where)
       (-run-db-id-update conn db ir opts)
       (-run-normal-update conn db ir opts)))))
