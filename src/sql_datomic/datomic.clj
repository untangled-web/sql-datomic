(ns sql-datomic.datomic
  (:require [datomic.api :as d]
            [clojure.edn :as edn]
            [com.stuartsierra.component :as component]
            [clojure.pprint :as pp]
            [clojure.walk :as walk]))

(def default-connection-uri "datomic:mem://dellstore")

(defn create-default-db []
  (d/create-database default-connection-uri)
  (let [connection (d/connect default-connection-uri)
        load-edn (comp read-string slurp)
        schema-tx (load-edn "resources/dellstore-schema.edn")
        customers-tx (load-edn "resources/dellstore-customers-data.edn")
        products-tx (load-edn "resources/dellstore-products-data.edn")
        orders-tx (load-edn "resources/dellstore-orders-data.edn")]
    @(d/transact connection schema-tx)
    ;; Order here matters.
    @(d/transact connection customers-tx)
    @(d/transact connection products-tx)
    @(d/transact connection orders-tx)
    connection))

(defn delete-default-db []
  (d/delete-database default-connection-uri))

(defn recreate-default-db []
  (delete-default-db)
  (create-default-db))

(def default-uri? (partial = default-connection-uri))

(defrecord DatomicConnection [connection-uri connection]
  component/Lifecycle
  (start [component]
    (if (default-uri? (:connection-uri component))
      (assoc component :connection (create-default-db))
      (assoc component :connection (d/connect (:connection-uri component)))))
  (stop [component]
    (when (default-uri? (:connection-uri component))
      (delete-default-db))
    (assoc component :connection nil)))

(defn system [{:keys [connection connection-uri]
               :or {connection-uri default-connection-uri}}]
  (component/system-map
   :datomic (map->DatomicConnection {:connection connection
                                     :connection-uri connection-uri})))

(comment

  (use 'clojure.repl)

  (def cxn (recreate-default-db))

  (defn so-touchy [entity]
    (walk/prewalk
     (fn [n]
       (if (= datomic.query.EntityMap (class n))
         (->> n d/touch (into {}))
         n))
     entity))

  (def order2
    (->> [:order/orderid 2]
         (d/entity (d/db cxn))
         d/touch))

  (->> order2 so-touchy pp/pprint)

  (def order5
    (->> [:order/orderid 5]
         (d/entity (d/db cxn))
         d/touch))

  (->> order5 so-touchy pp/pprint)

  (def sys (.start (system {})))
  (defn sys-cxn [] (->> sys :datomic :connection))

  )
