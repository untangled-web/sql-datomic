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

(defn select-ir->clj [ir]
  {:pre [(= :select (:type ir))]}
  (let [{:keys [fields tables where]} ir]
    ))

(defn map-eq-to-datomic-clause [ir-eq-clause]
  )

(defn table-column->attr-kw [{:keys [table column]}]
  (keyword table column))

;; TODO: need an env-like registry that keeps mapping from:
;;   {:table t, :column c}
;; to:
;;   ?e
;; TODO: how do we deal with table aliases?

(defn extract-columns [where-clauses]
  (let [column? (fn [v] (and (map? v)
                             (= #{:table :column}
                                (set (keys v)))))
        extract-from-clause (fn [clause]
                              (->> (tree-seq coll? seq clause)
                                   (filter column?)))]
    (->> where-clauses
         (mapcat extract-from-clause)
         (into #{}))))

(defn gensym-datomic-entity-var []
  (gensym "?e"))

(defn gensym-datomic-value-var []
  (gensym "?v"))

(defn build-datomic-var-map [columns]
  (let [name->entity (->> columns
                          (group-by :table)
                          (map (fn [[name _]]
                                 [name (gensym-datomic-entity-var)]))
                          (into {}))]
    (->> columns
         (map (fn [col] [col {:entity (get name->entity (:table col))
                              :value (gensym-datomic-value-var)
                              :attr (table-column->attr-kw col)}]))
         (into {}))))

(defn binary-comparison->datomic [col->var op vs]
  (let [[c v] vs
        {v-sym :value} (get col->var c :unknown-column!)
        kw->op {:= '=
                :not= 'not=
                :< '<
                :> '>
                :<= '<=
                :>= '>=}]
    [(list (kw->op op) v-sym v)]))

(defn between->datomic [col->var [c v1 v2]]
  (let [{v-sym :value} (get col->var c :unknown-column!)]
    ;; datomic's builtin <= is an extension and supports only two args.
    [(list 'clojure.core/<= v1 v-sym v2)]))

(defn where->datomic [clauses]
  {:pre [(not (empty? clauses))
         (every? list? clauses)
         (every? (comp keyword? first) clauses)
         (every? (fn [c]
                   (-> (first c)
                       #{:between := :not= :< :> :<= :>=}))
                 clauses)]}
  (let [col->var (->> clauses
                      extract-columns
                      build-datomic-var-map)
        base-where (->> col->var
                        (map (fn [[c {:keys [entity value]}]]
                               [entity (table-column->attr-kw c) value]))
                        (into []))]
    (->> clauses
         (map (fn [c]
                (let [[op & operands] c]
                  (case op
                    :between (between->datomic col->var operands)

                    (:= :not= :< :> :<= :>=)
                    (binary-comparison->datomic col->var op operands)

                    (throw (ex-info "unknown where-clause operator"
                                    {:operator op
                                     :operands operands
                                     :clause c}))))))
         (into base-where))))

(defn scrape-entities [dat-where]
  (->> dat-where
       (filter (fn [[e]]
                 (and (symbol? e)
                      (re-seq #"^\?e\d+" (name e)))))
       (map first)
       (into #{})))

(defn where->datomic-q [where]
  (let [ws (where->datomic where)
        es (scrape-entities ws)]
    `[:find ~@es
      :where ~@ws]))

(comment

  (use 'clojure.repl)

  ;; (def cxn (recreate-default-db))

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

  (def where-clauses
    [(list :between {:table "product", :column "price"} 10 15)
     (list :not= {:table "product", :column "title"} "AGENT CELEBRITY")])

  (where->datomic where-clauses)
  #_[[?e13699 :product/price ?v13700] [?e13699 :product/title ?v13701] [(clojure.core/<= 10 ?v13700 20)] [(not= ?v13701 "AGENT CELEBRITY")]]
  (where->datomic-q where-clauses)
  (d/q (where->datomic-q where-clauses) (d/db (sys-cxn)))
  (def ids (d/q (where->datomic-q where-clauses) (d/db (sys-cxn))))
  (mapcat identity ids)
  (->> ids
       (mapcat identity)
       (map (fn [id]
              (->> id
                   (d/entity (d/db (sys-cxn)))
                   d/touch))))

  (where->datomic
   [(list := {:table "product" :column "prod-id"} 42)])

  )
