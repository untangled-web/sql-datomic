(ns sql-datomic.datomic
  (:require [datomic.api :as d]
            [clojure.edn :as edn]))

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
    @(d/transact connection customers-tx)
    @(d/transact connection products-tx)
    @(d/transact connection orders-tx)
    connection))

(defn delete-default-db []
  (d/delete-database default-connection-uri))

(defn recreate-default-db []
  (delete-default-db)
  (create-default-db))
