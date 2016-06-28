(ns sql-datomic.util
  (:require [clojure.string :as str]
            [clojure.pprint :as pp]))

(defn vec->list [v]
  (->> v rseq (into '())))

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
