(ns sql-datomic.repl
  (:require [sql-datomic.parser :as parser]))

(def ^:dynamic *prompt* "sql> ")

(defn repl []
  (print *prompt*)
  (flush)
  (when-let [input (read-line)]
    (prn (parser/parser input))
    (recur)))

(defn -main [& args]
  (repl))
