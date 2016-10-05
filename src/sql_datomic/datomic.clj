(ns sql-datomic.datomic
  (:require [datomic.api :as d]
            [clojure.edn :as edn]
            [com.stuartsierra.component :as component]
            [clojure.pprint :as pp]
            [clojure.walk :as walk]
            [sql-datomic.util :as util :refer [get-entities-by-eids]])
  (:import [datomic.impl Exceptions$IllegalArgumentExceptionInfo]))

(def default-connection-uri "datomic:mem://somewhere")

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

(declare datomicify-clause)

(defn -create-dellstore-db []
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

(defn -create-starfighter-db []
  (d/create-database default-connection-uri)
  (let [connection (d/connect default-connection-uri)
        load-edn (fn [path]
                   (->> path
                        slurp
                        (edn/read-string {:readers *data-readers*})))
        schema-tx (load-edn "resources/starfighter-schema.edn")
        data-tx (load-edn "resources/starfighter-data.edn")]
    @(d/transact connection schema-tx)
    @(d/transact connection data-tx)
    connection))

(defn -create-seattle-db []
  (d/create-database default-connection-uri)
  (let [connection (d/connect default-connection-uri)
        load-edn (fn [path]
                   (->> path
                        slurp
                        (edn/read-string {:readers *data-readers*})))
        schema-tx (load-edn "resources/seattle-schema.edn")
        data0-tx (load-edn "resources/seattle-data0.edn")
        data1-tx (load-edn "resources/seattle-data1.edn")]
    @(d/transact connection schema-tx)
    @(d/transact connection data0-tx)
    @(d/transact connection data1-tx)
    connection))

(defn create-default-db
  ([] (create-default-db :dellstore))
  ([which-schema]
   (case which-schema
     :starfighter (-create-starfighter-db)
     :seattle (-create-seattle-db)
     :dellstore (-create-dellstore-db)
     ;; else
     (-create-dellstore-db))))

(defn delete-default-db []
  (d/delete-database default-connection-uri))

(defn recreate-default-db
  ([] (recreate-default-db :dellstore))
  ([which-schema]
   (delete-default-db)
   (create-default-db which-schema)))

(def default-uri? (partial = default-connection-uri))

(defrecord DatomicConnection [connection-uri connection schema-name]
  component/Lifecycle
  (start [component]
    (if (default-uri? (:connection-uri component))
      (assoc component :connection (create-default-db schema-name))
      (assoc component :connection (d/connect (:connection-uri component)))))
  (stop [component]
    (when (default-uri? (:connection-uri component))
      (delete-default-db))
    (assoc component :connection nil)))

(defn system [{:keys [connection connection-uri schema-name]
               :or {connection-uri default-connection-uri}}]
  (component/system-map
   :datomic (map->DatomicConnection {:connection connection
                                     :connection-uri connection-uri
                                     :schema-name schema-name})))

(defn table-column->attr-kw [{:keys [table column]}]
  (keyword table column))

(defonce db-id-column {:table "db", :column "id"})

(defn db-id? [c] (= c db-id-column))

(defn column? [v]
  (and (map? v)
       (= #{:table :column}
          (set (keys v)))))

(defn extract-columns
  "Returns a set of `{:table t, :column c}`."
  [where-clauses]
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

(defn datomic-var? [v]
  (and (symbol? v) (re-seq #"^\?" (name v))))

(defn scrape-datomic-vars [tree]
  (->> (tree-seq coll? seq tree)
       (filter datomic-var?)
       (into #{})))

(defn tag-entity-var [var name]
  (vary-meta var update :ir (fnil conj #{}) name))

(defn build-datomic-var-map
  "Returns a map, keyed by `{:table t, :column c}` with vals
   `{:entity ?e1234, :value ?v2341, :attr :foo/bar}`"
  [columns]
  ;; TODO: How do we deal with table aliases?
  ;;       This will naively unify based on table name alone.
  (let [name->entity (->> columns
                          (group-by :table)
                          (map (fn [[name _]]
                                 [name (-> (gensym-datomic-entity-var)
                                           (tag-entity-var name))]))
                          (into {}))]
    (->> columns
         (map (fn [col] [col {:entity (get name->entity (:table col))
                              :value (gensym-datomic-value-var)
                              :attr (table-column->attr-kw col)}]))
         (into {}))))

(defn binary-comparison->datomic
  [{:keys [col->var ident-env op operands]}]
  (let [[c v] operands
        {c-sym :value} (get col->var c :unknown-column!)
        v' (if-let [v-sym (get col->var v)]
             (:value v-sym)
             (get-in ident-env [v :var] v))
        kw->op {:= '=
                :not= 'not=
                :< '<
                :> '>
                :<= '<=
                :>= '>=}]
    [(list (kw->op op) c-sym v')]))

(defn between->datomic [{:keys [col->var operands]}]
  (let [[c v1 v2] operands
        {v-sym :value} (get col->var c :unknown-column!)]
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

(defn ->squashable [& args]
  {:squashable (vec args)})

(defn squashable? [v]
  (and (map? v) (= 1 (count v)) (-> v :squashable vector?)))

;; TODO: remove this
(defn db-id->datomic [{:keys [operands]}]
  (let [id (first operands)
        e-var (gensym-datomic-entity-var)]
    (->squashable [(list 'ground id) e-var]
                  [e-var])))

(defn build-datomic-ident-var-map
  "Returns a map, keyed by idents with vals `{:eid eid, :var ?id4321}`."
  [ident->eid]
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
        (if (squashable? v)
          (recur vs (apply conj result (:squashable v)))
          (recur vs (conj result v)))))))

(defn in->datomic [{:keys [col->var operands]}]
  (let [[c vs] operands
        attr (table-column->attr-kw c)
        {e-sym :entity} (get col->var c :unknown-column!)
        clausen (map (fn [v] [e-sym attr v]) vs)]
    (util/vec->list (into ['or] clausen))))

(defn and->datomic [{:keys [operands] :as env}]
  ;; Assumes any toplevel :and in :where has been raised.
  ;; Therefore, this should only be called within the scope
  ;; of a `or-join`.
  (let [env' (dissoc env :op :operands)
        dat-clausen (map (partial datomicify-clause env') operands)]
    (->> dat-clausen
         (into ['and])
         util/vec->list)))

(defn or->datomic [{:keys [operands] :as env}]
  (let [env' (dissoc env :op :operands)
        dat-clausen (map (partial datomicify-clause env') operands)
        vars (scrape-datomic-vars dat-clausen)]
    (->> dat-clausen
         (into ['or-join (vec vars)])
         util/vec->list)))

;; TODO: This function is very similar to `and->datomic`; refactor?
(defn not->datomic [{:keys [operands] :as env}]
  (let [env' (dissoc env :op :operands)
        dat-clausen (map (partial datomicify-clause env') operands)]
    (->> dat-clausen
         (into ['not])
         util/vec->list)))

(defn tag-clause [clause c]
  (vary-meta clause assoc :ir c))

(defn build-where-backbone [db clauses]
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
                               (-> [entity (table-column->attr-kw c) value]
                                   (tag-clause c))))
                        (into []))
        ident-where (->> ident-env
                         (map (fn [[ident {:keys [eid var]}]]
                                (list 'unify-ident ident var))))
        base-where (apply conj base-where ident-where)]
    {:col->var col->var
     :ident->eid ident->eid
     :ident-env ident-env
     :base-where base-where
     :ident-where ident-where}))

(defn datomicify-clause [env clause]
  (let [[op & operands] clause
        args {:col->var (:col->var env)
              :ident-env (:ident-env env)
              :op op
              :operands operands}]
    (case op
      :between (between->datomic args)

      (:= :not= :< :> :<= :>=) (binary-comparison->datomic args)

      :db-id (db-id->datomic args)

      :in (in->datomic args)

      :and (and->datomic args)

      :or (or->datomic args)

      :not (not->datomic args)

      (throw (ex-info "unknown where-clause operator"
                      {:operator op
                       :operands operands
                       :clause clause})))))

(defn where->datomic [db clauses]
  {:pre [(not (empty? clauses))
         (every? list? clauses)
         (every? (comp keyword? first) clauses)
         (every? (fn [c]
                   (-> (first c)
                       #{:between :in
                         := :not= :< :> :<= :>=
                         :db-id
                         :and :or :not}))
                 clauses)]}
  (let [{:keys [col->var ident-env ident-where
                base-where]} (build-where-backbone db clauses)
        clause-env {:col->var col->var :ident-env ident-env}]
    (->> clauses
         (map (partial datomicify-clause clause-env))
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

(defn db-id-clause? [clauses]
  (some set? clauses))

(defn db-id-clause-ir->eids [ir]
  (->> ir :where first))

(defn fields-ir->attrs [fields]
  (->> fields
       (map (fn [v]
              (if (column? v)
                (table-column->attr-kw v)
                v)))
       (into [])))

(defn qualified-*-attr? [v]
  (and (keyword? v)
       (= "*" (name v))
       (seq (namespace v))))

(defn gather-attrs-from-entities [entities]
  (->> entities (mapcat keys) (into #{})))

(defn -resolve-qualified-attrs [qualified-*-kw attrs]
  {:pre [(qualified-*-attr? qualified-*-kw)
         (coll? attrs)
         (every? keyword? attrs)]}
  (let [nspace (namespace qualified-*-kw)]
    (->> attrs
         (map (fn [attr]
                {:attr attr :nm (name attr) :ns (namespace attr)}))
         (filter (fn [{:keys [ns]}] (= ns nspace)))
         (remove (fn [{:keys [nm]}] (= "*" nm)))
         (map :attr))))

(defn resolve-qualified-attrs [qualified-*-kws attrs]
  (let [qualified-*-kws (if (coll? qualified-*-kws)
                          qualified-*-kws
                          [qualified-*-kws])
        q*-attrs (->> qualified-*-kws
                      (filter qualified-*-attr?)
                      (into #{}))]
    (->> q*-attrs
         (map (fn [q*] (-resolve-qualified-attrs q* attrs)))
         flatten
         (into #{}))))

(defn resolve-attrs [given avail]
  (let [univ (into #{} avail)]
    ;; preserve given field order
    (->> given
         (mapcat (fn [attr]
                   (if (qualified-*-attr? attr)
                     (-resolve-qualified-attrs attr univ)
                     [attr])))
         distinct)))

(defn supplement-with-consts [consts entities]
  (let [cs (mapcat (partial repeat 2) consts)]
    (for [e entities]
      (let [m (into {} e)]
        (if (seq cs)
          (apply assoc m cs)
          m)))))

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

#_(defn get-entities-by-eids [db eids]
  (for [eid eids]
    (->> eid
         (d/entity db)
         d/touch)))

(defn genuine-entity? [entity]
  "True iff arg is an entity with attributes.

  `datomic.api/entity` will return an entity object even if the database
  had no entity with the given eid.  This predicate's purpose is to help
  disambiguate the yes-we-found-your-entity results from this other.
  "
  (and (isa? (class entity) datomic.query.EntityMap)
       (not= (->> entity keys (into #{}))
             #{})))

(defn keep-genuine-entities [entities]
  (->> entities
       (map d/touch)
       (filter genuine-entity?)))

(defn delete-eids->tx-data [eids]
  (->> eids
       (map (fn [id] [:db.fn/retractEntity id]))
       (into [])))

(defn add-tempid [entity]
  (assoc entity :db/id (d/tempid :db.part/user)))

(defn insert-ir-traditional->tx-data [{:keys [table cols vals] :as ir}]
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

(defn insert-ir-assign-pairs->tx-data [{:keys [assign-pairs] :as ir}]
  {:pre [(seq assign-pairs)
         (vector? assign-pairs)
         (every? vector? assign-pairs)
         (every? (comp column? first) assign-pairs)
         (every? (comp (complement nil?) second) assign-pairs)]}
  (->> assign-pairs
       (map (fn [[c v]] [(table-column->attr-kw c) v]))
       (into {})
       add-tempid
       vector))

(defn insert-ir->tx-data [ir]
  (if (:assign-pairs ir)
    (insert-ir-assign-pairs->tx-data ir)
    (insert-ir-traditional->tx-data ir)))

(defn scrape-inserted-eids [transact-result]
  {:pre [(map? transact-result)]}
  (->> transact-result :tempids vals (into [])))

(defn hydrate-entity [db entity-id]
  (let [entity (->> entity-id (d/entity db) d/touch)]
    (into {:db/id (:db/id entity)} entity)))

(defn -enumerated-key? [k]
  (and (keyword? k) (re-seq #"--#\d+$" (name k))))

(defn -decompose-key [k]
  (if (-enumerated-key? k)
    (if-let [match (re-matches #"^(.*)--#(\d+)$" (name k))]
      (let [[_ base-name num] match]
        {:name-space (namespace k)
         :base-name base-name
         :num (Long/parseLong num)})
      (throw (ex-info "unable to split enumerated key" {:key k})))
    {:name-space (namespace k)
     :base-name (name k)
     :num 1}))

(defn -enumerate-key [k]
  (let [{:keys [name-space base-name num]} (-decompose-key k)]
    (keyword name-space (str base-name "--#" (inc num)))))

(defn -merge-uniq-key [m1 m2]
  {:pre [(map? m1)
         (map? m2)]}
  (let [seen (-> m1 keys set)]
    (->> m2
         (map (fn [[k v]]
                (let [k' (if (seen k)
                           (->> (iterate -enumerate-key k)
                                (drop-while seen)
                                first)
                           k)]
                  [k' v])))
         (into m1))))

(defn merge-with-enumerated-keys [& ms]
  (reduce -merge-uniq-key ms))

(defn hydrate-results [db relations]
  {:pre [(or (set? relations)
             (isa? (class relations) java.util.HashSet))
         (every? vector? relations)
         (every? (partial every? integer?) relations)]}
  (for [tuple relations]
    (let [ent-maps (map (partial hydrate-entity db) tuple)]
      ;; Combine each entity in this "row" into a single row.
      (if (seq ent-maps)
        (apply merge-with-enumerated-keys ent-maps)
        {}))))

;; Note: `e a v` form is required, including the assumed current value.
;; @(d/transact conn [[:db/retract 17592186045444 :product/uuid
;;                     #uuid "57607426-cdd4-49fa-aecb-0a2572976db9"]])
;; Note: If value has changed, then that :db/retract is ignored, but
;;       any other :db/retract's in the given transaction vector
;;       will be carried out.
(defn retract-ir->tx-data
  ([db {:keys [ids] :as ir}]
   (let [entities (->> (util/get-entities-by-eids db ids)
                       keep-genuine-entities)]
     (retract-ir->tx-data db ir entities)))

  ([db {:keys [attrs] :as ir} entities]
   (let [kws (map table-column->attr-kw attrs)]
     (->> (for [e entities
                kw kws]
            (let [eid (:db/id e)
                  v (get e kw)]
              [:db/retract eid kw v]))
          (into [])))))

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
  (require '[sql-datomic.parser :as par] :reload)

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

  (->> (d/q '[:find
              [?e3160 ...]
              :in
              $
              %
              :where
              [?e3160 :product/prod-id ?v3161]
              [?e3160 :product/category ?v3162]
              (unify-ident :product.category/action ?ident3163)
              [(> ?v3161 1000)]
              [(= ?v3162 ?ident3163)]]
            db rules)
       (map bloom)
       (map (juxt :product/prod-id :product/category))
       sort)
  (->> (d/q '[:find
              [?e3160 ...]
              :in
              $
              %
              :where
              [?e3160 :product/prod-id ?v3161]
              [?e3160 :product/category ?v3162]
              (unify-ident :product.category/action ?ident3163)
              (or-join [?v3161 ?v3162 ?ident3163]
                       [(< ?v3161 5000)]
                       [(= ?v3162 ?ident3163)])]
            db rules)
       (map bloom)
       (map (juxt :product/prod-id :product/category))
       sort)
  ;; If used with just a plain `or`, we get this error:
  ;;   AssertionError Assert failed: All clauses in 'or' must use same set of vars, had [#{?v3161} #{?v3162 ?ident3163}]
  ;; If used with `or-join`, then seems to work.
  ;; Implies the need to scrape `or-join` clauses for `?vars`.

  ;; Regarding `not` vs `not-join`, `not` does not seem to have the
  ;; same limitations re binding as does `or`.  In other words, this:
  ;; (not (or-join [?v3161 ?v3162 ?ident3163]
  ;;               [(< ?v3161 5000)]
  ;;               [(= ?v3162 ?ident3163)]))
  ;; is just fine.  Using `not-join` with the same bindings as
  ;; `or-join` also works but seems to be unnecessary.

  )
