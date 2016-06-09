(ns sql-datomic.repl
  (:require [sql-datomic.parser :as parser]
            [sql-datomic.datomic :as dat]
            [clojure.pprint :as pp]
            [clojure.tools.cli :as cli]))

(def ^:dynamic *prompt* "sql> ")

(defn repl []
  (print *prompt*)
  (flush)
  (let [input (read-line)]
    (when-not input
      (System/exit 0))
    (when (re-seq #"^(?ims)\s*(?:quit|exit)\s*$" input)
      (System/exit 0))
    (when (re-seq #"(?ms)\S" input)
      (pp/pprint (parser/parser input)))
    (recur)))

(defn -main [& args]
  (let [[opts args banner]
        (cli/cli args
                 ["-h" "--help" "Print this help"
                  :flag true
                  :default false]
                 ["-d" "--debug" "Write debug info to stderr"
                  :flag true
                  :default false]
                 ["-u" "--connection-uri"
                  "URI to Datomic DB; if missing, uses default mem db"])]
    (when (:help opts)
      (println banner)
      (System/exit 0))

    (def sys (.start (dat/system opts)))

    (let [uri (->> sys :datomic :connection-uri)]
      (println "connected to:" uri)
      (when (dat/default-uri? uri)
        (println "*** using default in-mem database ***"))
      (println "type `exit` or `quit` or ^D to exit"))
    (repl)))
