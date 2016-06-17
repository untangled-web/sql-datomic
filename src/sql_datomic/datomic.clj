(ns sql-datomic.datomic
  (:require [datomic.api :as d]
            [clojure.edn :as edn]
            [com.stuartsierra.component :as component]
            [clojure.pprint :as pp]
            [clojure.walk :as walk])
  (:import [datomic.impl Exceptions$IllegalArgumentExceptionInfo]))

(def default-connection-uri "datomic:mem://dellstore")

(def rules
  '[
    ;; (between 2 ?foo-id 4)
    ;; (between #inst "2004-01-09T00:00:00.000-00:00"
    ;;      ?od #inst "2004-01-10T00:00:00.000-00:00"
    [[between ?v1 ?c ?v2]
     [(<= ?v1 ?c)]
     [(<= ?c ?v2)]]

    ;; (unify-ident :product.category/action ?ident2974)
    [[unify-ident ?ident ?var]
     [(datomic.api/entid $ ?ident) ?var]]

    ;;
    #_[[db-id= ?eid ?var]
     [(ground ?eid) ?var]
     [?var]]
    ])

(defn create-default-db []
  (d/create-database default-connection-uri)
  (let [connection (d/connect default-connection-uri)
        load-edn (fn [path]
                   (->> path
                        slurp
                        (edn/read-string {:readers *data-readers*})))
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

(defn table-column->attr-kw [{:keys [table column]}]
  (keyword table column))

(defonce db-id-column {:table "db", :column "id"})
(defn db-id? [c] (= c db-id-column))

(defn column? [v]
  (and (map? v)
       (= #{:table :column}
          (set (keys v)))))

(defn extract-columns [where-clauses]
  (let [extract-from-clause (fn [clause]
                              (->> (tree-seq coll? seq clause)
                                   (filter column?)))]
    (->> where-clauses
         (mapcat extract-from-clause)
         (into #{}))))

(defn gensym-datomic-entity-var []
  (gensym "?e"))

(defn gensym-datomic-value-var []
  (gensym "?v"))

(defn gensym-datomic-ident-var []
  (gensym "?ident"))

(defn build-datomic-var-map [columns]
  ;; TODO: How do we deal with table aliases?
  ;;       This will naively unify based on table name alone.
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

(defn binary-comparison->datomic [col->var ident-env op vs]
  (let [[c v] vs
        {v-sym :value} (get col->var c :unknown-column!)
        v (get-in ident-env [v :var] v)
        kw->op {:= '=
                :not= 'not=
                :< '<
                :> '>
                :<= '<=
                :>= '>=}]
    [(list (kw->op op) v-sym v)]))

(defn between->datomic [col->var [c v1 v2]]
  (let [{v-sym :value} (get col->var c :unknown-column!)]
    (list 'between v1 v-sym v2)))

(defn lookup-ref? [r]
  (and (vector? r)
       (->> r first keyword?)
       (->> r second ((complement coll?)))))

(defn ident-value [db v]
  (and v
       (not (number? v)) ;; skip eids
       (or (keyword? v) (lookup-ref? v))
       (try
         (d/entid db v)
         (catch Exceptions$IllegalArgumentExceptionInfo _ex
           nil))))

(defn extract-ident-values [db tree]
  (->> tree
       (tree-seq coll? seq)
       (filter (fn [v] (ident-value db v)))))

;; FIXME: Has trouble with unification when other clauses are present.
(defn db-id->datomic [id]
  (let [e-var (gensym-datomic-entity-var)]
    [[(list 'ground id) e-var]
     [e-var]]))

(defn build-datomic-ident-var-map [ident->eid]
  (->> ident->eid
       (map (fn [[ident eid]]
              [ident {:eid eid
                      :var (gensym-datomic-ident-var)}]))
       (into {})))

(defn squoosh [cs]
  (loop [remain cs, result []]
    (if-not (seq remain)
      result
      (let [[v & vs] remain]
        (if (and (vector? v) (vector? (first v)))
          (recur vs (apply conj result v))
          (recur vs (conj result v)))))))

(defn where->datomic [db clauses]
  {:pre [(not (empty? clauses))
         (every? list? clauses)
         (every? (comp keyword? first) clauses)
         (every? (fn [c]
                   (-> (first c)
                       #{:between := :not= :< :> :<= :>= :db-id}))
                 clauses)]}
  (let [col->var (->> clauses
                      extract-columns
                      build-datomic-var-map)
        ident->eid (->> clauses
                        (tree-seq coll? seq)
                        (map (fn [n] [n (ident-value db n)]))
                        (filter second)
                        (into {}))
        ident-env (build-datomic-ident-var-map ident->eid)
        base-where (->> col->var
                        (map (fn [[c {:keys [entity value]}]]
                               [entity (table-column->attr-kw c) value]))
                        (into []))
        ident-where (->> ident-env
                         (map (fn [[ident {:keys [eid var]}]]
                                (list 'unify-ident ident var))))
        base-where (apply conj base-where ident-where)]
    (->> clauses
         (map (fn [c]
                (let [[op & operands] c]
                  (case op
                    :between (between->datomic col->var operands)

                    (:= :not= :< :> :<= :>=)
                    (binary-comparison->datomic
                     col->var ident-env op operands)

                    :db-id (db-id->datomic (first operands))

                    (throw (ex-info "unknown where-clause operator"
                                    {:operator op
                                     :operands operands
                                     :clause c}))))))
         squoosh
         (into base-where))))

(defn scrape-entities [dat-where]
  (->> dat-where
       (filter (fn [[e]]
                 (and (symbol? e)
                      (re-seq #"^\?e\d+" (name e)))))
       (map first)
       (into #{})))

(defn where->datomic-q [db where]
  (let [ws (where->datomic db where)
        es (scrape-entities ws)]
    `[:find ~@es
      :in ~'$ ~'%
      :where ~@ws]))

(defn update-ir->base-tx-data [{:keys [assign-pairs] :as ir}]
  {:pre [(seq assign-pairs)
         (vector? assign-pairs)
         (every? vector? assign-pairs)
         (every? (comp column? first) assign-pairs)
         (every? (comp (complement nil?) second) assign-pairs)]}
  (->> assign-pairs
       (map (fn [[c v]] [(table-column->attr-kw c) v]))
       (into {})))

(defn stitch-tx-data [base-tx eids]
  {:pre [(map? base-tx)]}
  (->> eids
       (map (fn [id] (assoc base-tx :db/id id)))
       (into [])))

(defn get-entities-by-eids [db eids]
  (for [eid eids]
    (->> eid
         (d/entity db)
         d/touch)))

(defn delete-eids->tx-data [eids]
  (->> eids
       (map (fn [id] [:db.fn/retractEntity id]))
       (into [])))

(defn add-tempid [entity]
  (assoc entity :db/id (d/tempid :db.part/user)))

(defn insert-ir->tx-data [{:keys [table cols vals] :as ir}]
  {:pre [(seq cols)
         (seq vals)
         (seq table)
         (string? table)
         (every? string? cols)
         (= (count cols) (count vals))]}
  (let [attrs (->> cols (map (partial keyword table)))]
    (->> (zipmap attrs vals)
         add-tempid
         vector)))

(defn scrape-inserted-eids [transact-result]
  {:pre [(map? transact-result)]}
  (->> transact-result :tempids vals (into [])))

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
  (def db (d/db (sys-cxn)))
  (def bloom (comp d/touch (partial d/entity db)))

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

  (->> (d/q '[:find [?e ...]
              :in $
              :where
              [(ground 17592186045445) ?e]
              [?e]]
            db)
       (map bloom) )

  (def target (->> (d/q '[:find [?e ...]
                          :in $
                          :where
                          [?e :product/prod-id 1567]]
                        db)
                   (map bloom)
                   first))
  @(d/transact (sys-cxn) [{:db/id (:db/id target)
                           :product/tag :ace-meet-greet-feet-beet-leet
                           :product/special true}])
  (def db (d/db (sys-cxn)))
  (def bloom (comp d/touch (partial d/entity db)))
  (->> (d/q '[:find [?e ...]
              :in $
              :where
              [?e :product/prod-id 1567]]
            db)
       (map bloom)
       first
       (into {})
       pp/pprint)

  [:set_clausen
   [:assignment_pair
    {:table "product", :column "category"}
    :product.category/new]]

  )
