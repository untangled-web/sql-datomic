(ns sql-datomic.tabula
  (:require [clojure.pprint :as pp])
  (:import [datomic.query EntityMap]))

(defn entity-map? [e]
  (isa? (class e) EntityMap))

(defn abbreviate-entity-maps [entity]
  (->> entity
       (map (fn [[k v]]
              (let [v' (if (entity-map? v)
                         (select-keys v [:db/id])
                         v)]
                [k v'])))
       (into {})))

(def cardinality-many? set?)

(defn select-cardinality-many-attrs [entity]
  (->> entity
       (keep (fn [[k v]]
               (when (cardinality-many? v)
                 k)))
       sort
       vec))

(defn elide-cardinality-manys [entity]
  (->> entity
       (remove (fn [[_ v]] (cardinality-many? v)))
       (into {})))

(defn entity->printable-row [entity]
  (->> entity
       (map (fn [[k v]] [k (pr-str v)]))
       (into {})))

(def process-entity (comp entity->printable-row
                          abbreviate-entity-maps
                          elide-cardinality-manys))

(defn -print-elided-cardinality-many-attrs
  ([rows]
   (when (seq rows)
     (let [attrs (select-cardinality-many-attrs (first rows))]
       (when (seq attrs)
         (println "Elided cardinality-many attrs: " attrs)))))
  ([ks rows]
   (->> rows
        (map (fn [row] (select-keys row ks)))
        -print-elided-cardinality-many-attrs)))

(defn -print-row-count [rows]
  (printf "(%d rows)\n" (count rows)))

(defn -print-simple-table [{:keys [ks rows print-fn]}]
  (->> rows
       (map process-entity)
       (into [])
       print-fn)
  (-print-row-count rows)
  (if (seq ks)
    (-print-elided-cardinality-many-attrs ks rows)
    (-print-elided-cardinality-many-attrs rows)))

(defn print-simple-table
  ([ks rows]
   (-print-simple-table {:ks ks
                         :rows rows
                         :print-fn (partial pp/print-table ks)}))
  ([rows]
   (-print-simple-table {:rows rows
                         :print-fn pp/print-table})))

(defn print-expanded-table []
  )
