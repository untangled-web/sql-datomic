(ns sql-datomic.select-command
  (:require [sql-datomic.datomic :as dat]
            [sql-datomic.util :as util :refer [squawk]]
            [datomic.api :as d]
            [clojure.pprint :as pp]))

(defn -display-raw-entities [entities]
  (squawk "Entities")
  (binding [*out* *err*]
    (when-not (seq entities)
      (println "None"))
    (doseq [entity entities]
      (pp/pprint entity)
      (flush))))

(defn decorate-with-db-id [entity]
  ;; Note:  Has side-effect of turning entity into a map.
  (assoc (into {} entity) :db/id (:db/id entity)))

(defn enhance-entities [entities fields]
  (let [ms (map decorate-with-db-id entities)
        fattrs (dat/fields-ir->attrs fields)
        eattrs (dat/gather-attrs-from-entities ms)
        attrs (dat/resolve-attrs fattrs eattrs)
        consts (remove keyword? attrs)]
    {:entities (dat/supplement-with-consts consts ms)
     :attrs attrs}))

(defn -run-db-id-select
  [db {:keys [where fields] :as ir} {:keys [debug]}]
  (let [ids (dat/db-id-clause-ir->eids ir)
        es (util/get-entities-by-eids db ids)
        raw-entities (dat/keep-genuine-entities es)
        {:keys [entities attrs]} (enhance-entities raw-entities fields)]
    (when debug (-display-raw-entities raw-entities))
    {:ids ids
     :raw-entities raw-entities
     :entities entities
     :attrs attrs}))

(defn -run-normal-select
  [db {:keys [where fields]} {:keys [debug]}]
  (let [query (dat/where->datomic-q db where)]
    (when debug
      (squawk "Datomic Rules" dat/rules)
      (squawk "Datomic Query" query))
    (let [results (d/q query db dat/rules)]
      (when debug (squawk "Raw Results" results))
      (let [raw-entities (dat/hydrate-results db results)
            {:keys [entities attrs]} (enhance-entities raw-entities fields)]
        (when debug (-display-raw-entities raw-entities))
        {:query query
         ;; :ids ids
         :raw-entities raw-entities
         :entities entities
         :attrs attrs}))))

(defn run-select
  ([db ir] (run-select db ir {}))
  ([db {:keys [where fields] :as ir} opts]
   {:pre [(= :select (:type ir))]}
   (when (seq where)
     (if (dat/db-id-clause? where)
       (-run-db-id-select db ir opts)
       (-run-normal-select db ir opts)))))
