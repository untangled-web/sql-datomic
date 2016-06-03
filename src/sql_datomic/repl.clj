(ns sql-datomic.repl
  (:require [sql-datomic.parser :as parser]
            [clojure.pprint :as pp]))

(def ^:dynamic *prompt* "sql> ")

(defn repl []
  (print *prompt*)
  (flush)
  (when-let [input (read-line)]
    (pp/pprint (parser/parser input))
    (recur)))

(defn -main [& args]
  (repl))
