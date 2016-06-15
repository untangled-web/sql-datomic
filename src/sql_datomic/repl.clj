(ns sql-datomic.repl
  (:require [sql-datomic.parser :as parser]
            [sql-datomic.datomic :as dat]
            [clojure.pprint :as pp]
            [clojure.tools.cli :as cli]
            [datomic.api :as d]
            clojure.repl
            [clojure.string :as str]))

(def ^:dynamic *prompt* "sql> ")

(declare sys)

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

(defn repl [{:keys [debug] :as opts}]
  (let [dbg (atom debug)]
    (print *prompt*)
    (flush)
    (let [input (read-line)]
      (when-not input
        (System/exit 0))
      (when (re-seq #"^(?ims)\s*(?:quit|exit)\s*$" input)
        (System/exit 0))

      ;; FIXME: Behold, the great leaning tower of REPL code.
      (try
        (if (re-seq #"^(?i)\s*debug\s*$" input)
          (let [new-debug (not @dbg)]
            (println "Set debug to" (if new-debug "ON" "OFF"))
            (flush)
            (reset! dbg new-debug))

          (when (re-seq #"(?ms)\S" input)
            (let [maybe-ast (parser/parser input)]
              (if-not (parser/good-ast? maybe-ast)
                (squawk "Parse error" maybe-ast)
                (do
                  (when @dbg (squawk "AST" maybe-ast))
                  (let [ir (parser/transform maybe-ast)]
                    (when @dbg (squawk "Intermediate Repr" ir))
                    (when (= :select (:type ir))
                      (when-let [wheres (:where ir)]
                        (let [db (->> sys :datomic :connection d/db)
                              query (dat/where->datomic-q db wheres)]
                          (when @dbg
                            (squawk "Datomic Rules" dat/rules)
                            (squawk "Datomic Query" query))
                          (let [results (d/q query db dat/rules)]
                            (when @dbg (squawk "Raw Results" results))
                            (let [ids (mapcat identity results)]
                              (when @dbg
                                (squawk "Entities")
                                (when-not (seq results)
                                  (binding [*out* *err*] (println "None"))))
                              (doseq [id ids]
                                (let [entity (d/touch (d/entity db id))]
                                  (pp/pprint entity)
                                  (flush))))))))))))))
        (catch Exception ex
          (binding [*out* *err*]
            (println "\n!!! Error !!!")
            (if @dbg
              (clojure.repl/pst ex)
              (println (.toString ex)))
            (flush))))

      (recur (assoc opts :debug @dbg)))))

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
      (println "type `exit` or `quit` or ^D to exit")
      (println "type `debug` to toggle debug mode"))
    (repl opts)))
