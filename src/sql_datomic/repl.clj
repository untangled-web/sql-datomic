(ns sql-datomic.repl
  (:require [sql-datomic.parser :as parser]
            [sql-datomic.datomic :as dat]
            [sql-datomic.util :refer [squawk]]
            [sql-datomic.select-command :as sel]
            [clojure.pprint :as pp]
            [clojure.tools.cli :as cli]
            [datomic.api :as d]
            clojure.repl
            [clojure.string :as str]
            [sql-datomic.tabula :as tab]))

(def ^:dynamic *prompt* "sql> ")

(declare sys)

(defn pointer [s index]
  (str/join (conj (into [] (repeat (dec index) \space)) \^)))

(defn ruler [s]
  (let [nats (->> (range) (map inc) (take (count s)))
        ->line (fn [coll]
                 (->> coll (map str) str/join))
        ones (map (fn [n] (rem n 10))
                  nats)
        tens (map (fn [n] (if (zero? (rem n 10))
                            (rem (quot n 10) 10)
                            \space))
                  nats)]
    (str/join "\n" [(->line ones) (->line tens)])))

(defn print-ruler
  ([input] (print-ruler input nil))
  ([input index]
   (when (seq input)
     (binding [*out* *err*]
       (println "\nInput with column offsets:\n==========================")
       (println input)
       (when index
         (println (pointer input index)))
       (println (ruler input))
       (flush)))))

(defn -debug-display-entities [db ids]
  (let [entities (dat/get-entities-by-eids db ids)]
    (doseq [entity entities]
      (binding [*out* *err*]
        (pp/pprint entity)
        (flush)))))

(defn repl [{:keys [debug pretend expanded] :as opts}]
  (let [dbg (atom debug)
        loljk (atom pretend)
        x-flag (atom expanded)
        noop (atom false)]
    (print *prompt*)
    (flush)
    (let [input (read-line)]
      (when-not input
        (System/exit 0))
      (when (re-seq #"^(?ims)\s*(?:quit|exit)\s*$" input)
        (System/exit 0))

      ;; FIXME: Behold, the great leaning tower of REPL code.
      (try
        (when (re-seq #"^(?i)\s*debug\s*$" input)
          (let [new-debug (not @dbg)]
            (println "Set debug to" (if new-debug "ON" "OFF"))
            (flush)
            (reset! dbg new-debug)
            (reset! noop true)))
        (when (re-seq #"^(?i)\s*pretend\s*$" input)
          (let [new-pretend (not @loljk)]
            (println "Set pretend to" (if new-pretend "ON" "OFF"))
            (flush)
            (reset! loljk new-pretend)
            (reset! noop true)))
        (when (and (not @dbg) @loljk)
          (println "Set debug to ON due to pretend ON")
          (flush)
          (reset! dbg true)
          (reset! noop true))
        (when (re-seq #"^(?i)\s*\\x\s*$" input)
          (let [new-expand (not @x-flag)]
            (println "Set expanded display to" (if new-expand "ON" "OFF"))
            (flush)
            (reset! x-flag new-expand)
            (reset! noop true)))

        (when (and (not @noop) (re-seq #"(?ms)\S" input))
          (let [maybe-ast (parser/parser input)]
            (if-not (parser/good-ast? maybe-ast)
              (do
                (squawk "Parse error" maybe-ast)
                (when-let [hint (parser/hint-for-parse-error maybe-ast)]
                  (binding [*out* *err*]
                    (println (str "\n*** Hint: " hint))))
                (print-ruler input (:column maybe-ast)))
              (do
                (when @dbg (squawk "AST" maybe-ast))
                (let [conn (->> sys :datomic :connection)
                      db (d/db conn)
                      ir (parser/transform maybe-ast)]
                  (when @dbg (squawk "Intermediate Repr" ir))

                  ;; FIXME: Sooooo much copy-pasta ...
                  (case (:type ir)

                    :select
                    (let [result (sel/run-select db ir {:debug @dbg})
                          entities (:entities result)
                          attrs (:attrs result)
                          print-table-fn (if @x-flag
                                           tab/print-expanded-table
                                           tab/print-simple-table)]
                      (print-table-fn (seq attrs) entities)
                      (flush))

                    :update
                    (when-let [wheres (:where ir)]
                      (if (dat/db-id-clause? wheres)
                        (let [ids (dat/db-id-clause-ir->eids ir)
                              entities (dat/get-entities-by-eids db ids)]
                          (when @dbg
                            (squawk "Entities Targeted for Update"))
                          (println (if (seq ids) ids "None"))
                          (when @dbg (-debug-display-entities db ids))
                          (when (seq entities)
                            (let [base-tx (dat/update-ir->base-tx-data ir)
                                  tx-data (dat/stitch-tx-data base-tx ids)]
                              (when @dbg (squawk "Transaction" tx-data))
                              (if @loljk
                                (println
                                 "Halting transaction due to pretend mode ON")
                                (do
                                  (println)
                                  (println @(d/transact conn tx-data))
                                  (when @dbg
                                    (squawk "Entities after Transaction")
                                    (-debug-display-entities (d/db conn) ids)))))))
                        (let [query (dat/where->datomic-q db wheres)]
                          (when @dbg
                            (squawk "Datomic Rules" dat/rules)
                            (squawk "Datomic Query" query))
                          (let [results (d/q query db dat/rules)]
                            (when @dbg (squawk "Raw Results" results))
                            ;; FIXME: this is *not* the case, transacts on *all*
                            ;;        eids participating in joins.
                            ;; UPDATE pertains to only one "table" (i.e., no
                            ;; joins), so this flattened list of ids is okay.
                            (let [ids (mapcat identity results)]
                              (when @dbg
                                (squawk "Entities Targeted for Update"))
                              (println (if (seq ids) ids "None"))
                              (when @dbg (-debug-display-entities db ids))
                              (when (seq results)
                                (let [base-tx (dat/update-ir->base-tx-data ir)
                                      tx-data (dat/stitch-tx-data base-tx ids)]
                                  (when @dbg (squawk "Transaction" tx-data))
                                  (if @loljk
                                    (println
                                     "Halting transaction due to pretend mode ON")
                                    (do
                                      (println)
                                      (println @(d/transact conn tx-data))
                                      (when @dbg
                                        (squawk "Entities after Transaction")
                                        (-debug-display-entities (d/db conn) ids)))))))))))

                    :delete
                    (when-let [wheres (:where ir)]
                      (if (dat/db-id-clause? wheres)
                        (let [ids (dat/db-id-clause-ir->eids ir)
                              entities (dat/get-entities-by-eids db ids)]
                          (when @dbg
                            (squawk "Entities Targeted for Delete"))
                          ;; Always give indication of what will be deleted.
                          (println (if (seq ids) ids "None"))
                          (when @dbg
                            (-debug-display-entities db ids))
                          (when (seq entities)
                            (let [tx-data (dat/delete-eids->tx-data ids)]
                              (when @dbg (squawk "Transaction" tx-data))
                              (if @loljk
                                (println
                                 "Halting transaction due to pretend mode ON")
                                (do
                                  (println)
                                  (println @(d/transact conn tx-data))
                                  (when @dbg
                                    (squawk "Entities after Transaction")
                                    (-debug-display-entities (d/db conn) ids)))))))
                        (let [query (dat/where->datomic-q db wheres)]
                          (when @dbg
                            (squawk "Datomic Rules" dat/rules)
                            (squawk "Datomic Query" query))
                          (let [results (d/q query db dat/rules)]
                            (when @dbg (squawk "Raw Results" results))
                            ;; FIXME: this is *not* the case, transacts on *all*
                            ;;        eids participating in joins.
                            ;; DELETE pertains to only one "table" (i.e., no
                            ;; joins), so this flattened list of ids is okay.
                            (let [ids (mapcat identity results)]
                              (when @dbg
                                (squawk "Entities Targeted for Delete"))
                              ;; Always give indication of what will be deleted.
                              (println (if (seq ids) ids "None"))
                              (when @dbg
                                (-debug-display-entities db ids))
                              (when (seq results)
                                (let [tx-data (dat/delete-eids->tx-data ids)]
                                  (when @dbg (squawk "Transaction" tx-data))
                                  (if @loljk
                                    (println
                                     "Halting transaction due to pretend mode ON")
                                    (do
                                      (println)
                                      (println @(d/transact conn tx-data))
                                      (when @dbg
                                        (squawk "Entities after Transaction")
                                        (-debug-display-entities (d/db conn) ids)))))))))))

                    :insert
                    (let [tx-data (dat/insert-ir->tx-data ir)]
                      (when @dbg (squawk "Transaction" tx-data))
                      (if @loljk
                        (println
                         "Halting transaction due to pretend mode ON")
                        (do
                          (println)
                          (let [transact-result @(d/transact conn tx-data)
                                ids (dat/scrape-inserted-eids
                                     transact-result)]
                            (prn ids)
                            (when @dbg
                              (squawk "Entities after Transaction")
                              (-debug-display-entities (d/db conn) ids))))))

                    ;; else
                    (throw (ex-info "Unknown query type" {:type (:type ir)
                                                          :ir ir}))))))))
        (catch Exception ex
          (binding [*out* *err*]
            (println "\n!!! Error !!!")
            (if @dbg
              (do
                (clojure.repl/pst ex)
                (print-ruler input))
              (println (.toString ex)))
            (flush))))

      (recur (assoc opts
                    :debug @dbg
                    :pretend @loljk
                    :expanded @x-flag)))))

(defn -main [& args]
  (let [[opts args banner]
        (cli/cli args
                 ["-h" "--help" "Print this help"
                  :flag true
                  :default false]
                 ["-d" "--debug" "Write debug info to stderr"
                  :flag true
                  :default false]
                 ["-p" "--pretend" "Run without transacting; turns on debug"
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
      (println "type `debug` to toggle debug mode")
      (println "type `pretend` to toggle pretend mode")
      (println "type `\\x` to toggle extended display mode"))
    (repl opts)))
