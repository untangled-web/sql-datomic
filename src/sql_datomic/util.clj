(ns sql-datomic.util
  (:require [clojure.string :as str]
            [clojure.pprint :as pp]
            [datomic.api :as d]))

(defn vec->list [v]
  (->> v rseq (into '())))

(defn get-entities-by-eids [db eids]
  (for [eid eids]
    (->> eid
         (d/entity db)
         d/touch)))

(defn squawk
  ([title] (squawk title nil))
  ([title data]
   (let [s (str title ":")
         sep (str/join (repeat (count s) \=))]
     (binding [*out* *err*]
       (println (str "\n" s "\n" sep))
       (when data
         (pp/pprint data))
       (flush)))))

(defn -debug-display-entities [entities]
  (doseq [entity entities]
    (binding [*out* *err*]
      (pp/pprint entity)
      (flush))))

(defn -debug-display-entities-by-ids [db ids]
  (-debug-display-entities (get-entities-by-eids db ids)))

(defn ir-table-column? [x]
  (and (map? x) (string? (:table x)) (string? (:column x))))

(defn scrape-ir-table-columns [ir]
  (->> (tree-seq coll? seq ir)
       (filter ir-table-column?)
       (into #{})))

(defn summarize-ir-columns-by-table [ir]
  (group-by :table (scrape-ir-table-columns ir)))

(defn entity-var? [x]
  (and (symbol? x)
       (re-seq #"^\?e" (name x))))

(defn value-var? [x]
  (and (symbol? x)
       (re-seq #"^\?v" (name x))))

(defn base-clause? [x]
  (and (vector? x)
       (>= (count x) 3)
       (keyword? (nth x 1))
       (entity-var? (nth x 0))
       (value-var? (nth x 2))))

(defn scrape-dat-query->base-clauses [dat-query]
  (->> dat-query
       (drop-while #(not= % :where))
       rest
       (filter base-clause?)))

(defn scrape-dat-query->entity-vars [dat-query]
  (->> dat-query
       (drop-while #(not= % :find))
       rest
       (take-while #(not (#{:where :in :with} %)))
       (filter entity-var?)))

(defn summarize-base-clauses-by-entity-vars [dat-query]
  (group-by first (scrape-dat-query->base-clauses dat-query)))


;; Updates/Deletes should be carried out with respect to
;; one and only one "table".
(defn infer-entity-index
  ([{:keys [table] :as ir} dat-query]
   (infer-entity-index ir dat-query table))

  ([ir dat-query table]
   {:pre [(seq table)]}

   (let [entity-vars (scrape-dat-query->entity-vars dat-query)
         entity-vars->index (->> entity-vars
                                 (map-indexed (comp vec rseq vector))
                                 (into {}))
         base-clauses (scrape-dat-query->base-clauses dat-query)]
     (let [vars (->> entity-vars
                     (filter (fn [var]
                               (when-let [meta-tables (-> var meta :ir)]
                                 (get meta-tables table)))))]
       (case (count vars)
         0 (throw (ex-info "No entity vars associated with given table"
                           {:given-table table
                            :entity-vars entity-vars
                            :associations
                            (zipmap entity-vars
                                    (map #(-> % meta :ir) entity-vars))}))

         1 (->> vars first entity-vars->index)

         ;; else
         ;; Not sure how we would end up here.
         (throw (ex-info "Ambiguous association of entity vars with table"
                         {:given-table table
                          :entity-vars vars
                          :associations
                          (zipmap vars
                                  (map #(-> % meta :ir) vars))})))))))
