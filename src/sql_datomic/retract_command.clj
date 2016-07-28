(ns sql-datomic.retract-command
  (:require [sql-datomic.datomic :as dat]
            [sql-datomic.util :as util
             :refer [squawk -debug-display-entities]]
            [datomic.api :as d]
            [clojure.pprint :as pp]))

(defn -run-harness [{:keys [conn db ir options ids]}]
  (let [{:keys [debug pretend silent]} options
        {:keys [attrs]} ir
        entities (->> (util/get-entities-by-eids db ids)
                      dat/keep-genuine-entities)]
    (when debug (squawk "Entities Targeted for Attr Retraction"))
    (when-not silent
      (println (if (seq entities) ids "None")))
    (when debug (-debug-display-entities entities))

    (if-not (seq entities)
      {:ids ids
       :before entities
       :pretend pretend}

      (let [tx-data (dat/retract-ir->tx-data db ir entities)]
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

(defn -run-db-id-retract
  [conn db {:keys [ids] :as ir} opts]
  (-run-harness {:conn conn
                 :db db
                 :ir ir
                 :options opts
                 :ids ids}))

;; {:type :retract,
;;  :ids #{1234 42},
;;  :attrs
;;  [{:table "product", :column "category"}
;;   {:table "product", :column "uuid"}]}

(defn run-retract
  ([conn db ir] (run-retract conn db ir {}))
  ([conn db {:keys [ids attrs] :as ir} opts]
   {:pre [(= :retract (:type ir))]}
   (when (seq ids)
     (-run-db-id-retract conn db ir opts))))
