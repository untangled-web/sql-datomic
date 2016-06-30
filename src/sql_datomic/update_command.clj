(ns sql-datomic.update-command
  (:require [sql-datomic.datomic :as dat]
            [sql-datomic.util :as util
             :refer [squawk -debug-display-entities]]
            [datomic.api :as d]
            [clojure.pprint :as pp]))

(defn -run-harness [{:keys [conn db ir options ids]}]
  (let [{:keys [debug pretend]} options
        entities (->> (util/get-entities-by-eids db ids)
                      dat/keep-genuine-entities)]
    (when debug (squawk "Entities Targeted for Update"))
    (println (if (seq entities) ids "None"))
    (when debug (-debug-display-entities entities))

    (if-not (seq entities)
      {:ids ids
       :before entities
       :pretend pretend}

      (let [base-tx (dat/update-ir->base-tx-data ir)
            tx-data (dat/stitch-tx-data base-tx ids)]
        (when debug (squawk "Transaction" tx-data))
        (if pretend
          (do
            (println "Halting transaction due to pretend mode ON")
            {:ids ids
             :before entities
             :tx-data tx-data
             :pretend pretend})

          (let [result @(d/transact conn tx-data)]
            (println)
            (println result)
            (let [entities' (util/get-entities-by-eids (d/db conn) ids)]
              (when debug
                (squawk "Entities after Transaction")
                (-debug-display-entities entities'))
              {:ids ids
               :tx-data tx-data
               :before entities
               :after entities'
               :result result})))))))

(defn -run-db-id-update
  [conn db ir opts]
  (let [ids (dat/db-id-clause-ir->eids ir)]
    (-run-harness {:conn conn
                   :db db
                   :ir ir
                   :options opts
                   :ids ids})))

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

;; Updates should be carried out with respect to one and only one "table".
(defn infer-update-entity-index
  ([{:keys [table] :as ir} dat-query]
   (infer-update-entity-index ir dat-query table))

  ([{:keys [assign-pairs] :as ir} dat-query table]
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

(defn -run-normal-update
  [conn db {:keys [where table assign-pairs] :as ir}
           {:keys [debug] :as opts}]
  (let [query (dat/where->datomic-q db where)]
    (when debug
      (squawk "Datomic Rules" dat/rules)
      (squawk "Datomic Query" query))

    (let [results (d/q query db dat/rules)]
      (when debug (squawk "Raw Results" results))
      (cond
        (or (->> results seq not)
            (->> results first count (= 1)))
        (let [ids (mapcat identity results)]
          (-run-harness {:conn conn
                         :db db
                         :ir ir
                         :options opts
                         :ids ids}))

        ;; Assertion:  results is non-empty
        ;; Assertion:  tuples in results have arity > 1
        (seq table)
        (let [i (infer-update-entity-index ir query)
              ids (map (fn [row] (nth row i)) results)]
          (when debug
            (binding [*out* *err*]
              (printf "Narrowing entity vars to index: %d\n" i)
              (prn [:ids ids])))
          (-run-harness {:conn conn
                         :db db
                         :ir ir
                         :options opts
                         :ids ids}))

        ;; Assertion:  no given table
        :else
        (let [table-names (->> assign-pairs
                               summarize-ir-columns-by-table
                               keys)]
          (if (= 1 (count table-names))
            (let [t (first table-names)
                  i (infer-update-entity-index ir query t)
                  ids (map (fn [row] (nth row i)) results)]
              (when debug
                (binding [*out* *err*]
                  (printf "Narrowing entity vars to index: %d\n" i)
                  (prn [:ids ids])))
              (-run-harness {:conn conn
                             :db db
                             :ir ir
                             :options opts
                             :ids ids}))
            ;; else
            (throw
             (IllegalArgumentException.
              "\n!!! Missing update target. Necessary with joins. Aborting update !!!\n"))))))))

(comment

  (def ir
    '{:assign-pairs [[{:table "product", :column "special"} true]],
      :where
      [(:between {:table "product", :column "prod-id"} 1000 9000)
       (:=
        {:table "product", :column "prod-id"}
        {:table "orderline", :column "prod-id"})],
      :type :update})


  (def query
    '[:find
      ?e3572
      ?e3571
      :in
      $
      %
      :where
      [?e3571 :product/prod-id ?v3573]
      [?e3572 :orderline/prod-id ?v3574]
      (between 1000 ?v3573 9000)
      [(= ?v3573 ?v3574)]])


  )

(defn run-update
  ([conn db ir] (run-update conn db ir {}))
  ([conn db {:keys [where] :as ir} opts]
   {:pre [(= :update (:type ir))]}
   (when (seq where)
     (if (dat/db-id-clause? where)
       (-run-db-id-update conn db ir opts)
       (-run-normal-update conn db ir opts)))))
