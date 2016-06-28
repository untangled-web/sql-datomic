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
