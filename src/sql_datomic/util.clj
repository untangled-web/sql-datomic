(ns sql-datomic.util)

(defn vec->list [v]
  (->> v rseq (into '())))
