(ns sql-datomic.insert-command
  (:require [sql-datomic.datomic :as dat]
            [sql-datomic.util :as util :refer [squawk]]
            [datomic.api :as d]
            [clojure.pprint :as pp]))

(defn run-insert
  ([conn ir] (run-insert conn ir {}))
  ([conn ir {:keys [debug pretend silent] :as opts}]
   {:pre [(= :insert (:type ir))]}

   (let [tx-data (dat/insert-ir->tx-data ir)]
     (when debug (squawk "Transaction" tx-data))
     (if pretend
       (do
         (println "Halting transaction due to pretend mode ON")
         {:tx-data tx-data
          :pretend pretend})
       (let [result @(d/transact conn tx-data)
             ids (dat/scrape-inserted-eids result)]
         (when-not silent
           (println)
           (prn ids))
         (when debug
           (squawk "Entities after Transaction")
           (util/-debug-display-entities-by-ids (:db-after result) ids))
         {:tx-data tx-data
          :ids ids
          :result result})))))
