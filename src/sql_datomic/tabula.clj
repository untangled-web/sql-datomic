(ns sql-datomic.tabula
  (:require [clojure.pprint :as pp]
            [clojure.string :as str]
            [clojure.set :as set])
  (:import [datomic.query EntityMap]))

(defn entity-map? [e]
  (isa? (class e) EntityMap))

(defn abbreviate-entity [entity]
  (if (and (map? entity)
           (:db/id entity))
    (select-keys entity [:db/id])
    entity))

(defn abbreviate-entity-maps [entity]
  (->> entity
       (map (fn [[k v]]
              (let [v' (if (entity-map? v)
                         (abbreviate-entity v)
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

(defn abbreviate-cardinality-many-attrs [entity]
  (->> entity
       (map (fn [[k v]]
              (let [v' (if (cardinality-many? v)
                         (->> v (map abbreviate-entity) (into #{}))
                         v)]
                [k v'])))
       (into {})))

(defn elide-cardinality-manys [entity]
  (->> entity
       (remove (fn [[_ v]] (cardinality-many? v)))
       (into {})))

(defn string->single-quoted-string [s]
  (str \' (str/escape s {\' "\\'"}) \'))

(defn ->printable [v]
  (if (string? v)
    (string->single-quoted-string v)
    (pr-str v)))

(defn entity->printable-row [entity]
  (->> entity
       (map (fn [[k v]]
              [(->printable k) (->printable v)]))
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
   (let [ks' (filter keyword? ks)]
     (->> rows
          (map (fn [row] (select-keys row ks')))
          -print-elided-cardinality-many-attrs))))

(defn -print-row-count [rows]
  (printf "(%d rows)\n" (count rows)))

(defn -doctor-lead-row [rows]
  (if-not (seq rows)
    rows
    ;; pp/print-table will use the keys of the first row as a
    ;; template for the columns to display.
    (let [all-keys (->> rows
                        (mapcat keys)
                        (into #{}))
          lead-row (first rows)
          tail-rows (rest rows)
          missing-keys (set/difference all-keys
                                       (->> lead-row keys (into #{})))
          missing-map (->> missing-keys
                           (map (fn [k] [k nil]))
                           (into {}))
          doc-row (merge missing-map lead-row)]
      (into [doc-row] tail-rows))))

(defn -print-simple-table [{:keys [ks rows print-fn]}]
  (->> rows
       (map process-entity)
       (into [])
       -doctor-lead-row
       print-fn)
  (-print-row-count rows)
  (if (seq ks)
    (-print-elided-cardinality-many-attrs ks rows)
    (-print-elided-cardinality-many-attrs rows))
  (println))

(defn print-simple-table
  ([ks rows]
   (if (seq ks)
     (-print-simple-table
      {:ks ks
       :rows rows
       :print-fn (partial pp/print-table (map ->printable ks))})
     (print-simple-table rows)))
  ([rows]
   (-print-simple-table {:rows rows
                         :print-fn pp/print-table})))

(defn -print-expanded-table [{:keys [ks rows]}]
  (when (seq rows)
    (let [ks' (if (seq ks)
                ks
                (->> rows (mapcat keys) (into #{}) sort))
          pks (map ->printable ks')
          k-max-len (->> pks
                         (map (comp count str))
                         (sort >)
                         first)
          row-fmt (str "%-" k-max-len "s | %s\n")
          rows' (->> rows
                     (map (fn [row] (select-keys row ks')))
                     (map (comp entity->printable-row
                                abbreviate-entity-maps
                                abbreviate-cardinality-many-attrs))
                     (map-indexed vector))]
      (doseq [[i row] rows']
        (printf "-[ RECORD %d ]-%s\n"
                (inc i)
                (apply str (repeat 40 \-)))
        (let [xs (->>
                  ;; Need ->printable keys for lookup
                  ;; due to entity->printable-row applying ->printable
                  ;; to row keys.
                  pks
                  (map (fn [k] [k (get row k "")])))]
          (doseq [[k v] xs]
            (printf row-fmt k v))))))
  (-print-row-count rows)
  (println))

(defn print-expanded-table
  ([ks rows]
   (-print-expanded-table {:ks ks :rows rows}))
  ([rows]
   (-print-expanded-table {:rows rows})))
