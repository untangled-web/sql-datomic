(ns sql-datomic.schema
  (:require [datomic.api :as d]
            [clojure.pprint :as pp]))

;; Blacklist of keyword namespaces to elide when looking at attrs.
(def system-ns #{                  ;; s/ => untangled.datomic.schema
                 "confirmity"      ;; conformity migration lib
                 "constraint"      ;; s/reference-constraint-for-attribute
                 "datomic-toolbox" ;; from lib of same name
                 "db"
                 "db.alter"
                 "db.bootstrap"
                 "db.cardinality"
                 "db.excise"
                 "db.fn"
                 "db.install"
                 "db.lang"
                 "db.part"
                 "db.sys"
                 "db.type"
                 "db.unique"
                 "entity"         ;; s/entity-extensions
                 "fressian"
                 })

(defn get-user-schema
  "Returns seq of [eid attr-kw] pairs."
  [db]
  (d/q '[:find ?e ?ident
         :in $ ?system-ns
         :where
         [?e :db/ident ?ident]
         [(namespace ?ident) ?ns]
         [((complement contains?) ?system-ns ?ns)]]
       db system-ns))

(defn eid->map [db eid]
  (->> eid
       (d/entity db)
       d/touch
       (into {:db/id eid})))

(defn infer-schema [db]
  (let [e->m (partial eid->map db)]
    (->> (get-user-schema db)
         (map first)
         sort
         (map e->m))))

(defn tidy-schema [schema]
  (->> schema
       (sort-by :db/id)
       (map (fn [m] (dissoc m :db/id)))
       (into [])))

(defn looks-like-enum? [m]
  (and (map? m)
       (= #{:db/ident} (->> m keys (into #{})))
       (keyword? (:db/ident m))
       (re-seq #"[-\w]+\.[-\w]+/[-\w]+" (str (:db/ident m)))))

(defn group-as-entities
  [user-schema]
  (group-by (comp namespace :db/ident) user-schema))

(defn kw-enum->entity-name [k]
  {:pre [(keyword? k)]}
  (let [s (namespace k)]
    (if-let [m (re-matches #"^([-\w]+)\..+$" s)]
      (second m)
      nil)))

(defn group-enums-as-entities
  [enums]
  (->> enums
       (map :db/ident)
       (group-by kw-enum->entity-name)))

(defn summarize-schema [db]
  (let [s (->> db infer-schema tidy-schema)
        {e true, t false} (group-by (comp boolean looks-like-enum?) s)]
    {:tables (group-as-entities t)
     :enums (group-enums-as-entities e)}))

(defn infer-schema-of-entity [db e]
  (let [attrs (keys e)
        summary (summarize-schema db)
        t (->> summary :tables vals flatten)
        m (group-by :db/ident t)]
    (->> attrs
         (mapcat (fn [attr]
                   (get m attr))))))

(comment

  (use 'clojure.repl)
  (require '[sql-datomic.repl :as r])

  (->> r/db infer-schema tidy-schema pp/pprint)
  (def s (->> r/db infer-schema tidy-schema))
  (def enums (filter looks-like-enum? s))
  (def gu (group-as-entities s))
  (keys gu)
  (pp/pprint (get gu "product"))
  (pp/pprint (get gu "orderline"))
  (pp/pprint (get gu "order"))
  (def ge (group-enums-as-entities enums))
  (pp/pprint ge)

  )
