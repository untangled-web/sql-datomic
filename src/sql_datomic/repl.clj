(ns sql-datomic.repl
  (:require [sql-datomic.parser :as par]
            [sql-datomic.datomic :as dat]
            [sql-datomic.util :as util :refer [squawk]]
            [sql-datomic.select-command :as sel]
            [sql-datomic.update-command :as upd]
            [sql-datomic.delete-command :as del]
            [sql-datomic.insert-command :as ins]
            [sql-datomic.retract-command :as rtr]
            [clojure.pprint :as pp]
            [clojure.tools.cli :as cli]
            [datomic.api :as d]
            clojure.repl
            [clojure.string :as str]
            [clojure.set :as set]
            [sql-datomic.tabula :as tab]
            [sql-datomic.schema :as sch])
  ;; (:gen-class)
  )

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

(defn show-tables []
  (let [db (->> sys :datomic :connection d/db)
        summary (sch/summarize-schema db)
        table-names (->> summary :tables keys sort)]
    (doseq [t table-names]
      (println t)))
  (flush))

(defn -show-enums [es]
  (when (seq es)
    (println "Related Enums")
    (let [by-ns (group-by namespace es)]
      (doseq [k (-> by-ns keys sort)]
        (println (str/join " " (get by-ns k)))))))

(defn describe-table [name]
  (let [db (->> sys :datomic :connection d/db)
        summary (sch/summarize-schema db)
        table (get-in summary [:tables name])
        enums (get-in summary [:enums name])]
    (if-not (seq table)
      (println "Unknown table")

      (do
        (pp/print-table [:db/ident :db/valueType :db/cardinality
                         :db/unique :db/doc]
                        table)
        (-show-enums enums)))
    (flush)))

(defn describe-entity [eid]
  (let [db (->> sys :datomic :connection d/db)
        e (sch/eid->map db eid)
        s (sch/infer-schema-of-entity db e)]
    (if-not (seq s)
      (println "Unable to find schema")
      (pp/print-table [:db/ident :db/valueType :db/cardinality
                       :db/unique :db/doc]
                      s))
    (flush)))

(defn show-schema []
  (let [db (->> sys :datomic :connection d/db)
        summary (sch/summarize-schema db)
        table-map (get-in summary [:tables])
        ts (->> table-map (mapcat second) (sort-by :db/ident))
        enums-map (get-in summary [:enums])
        es (->> enums-map (mapcat second) (sort-by :db/ident))]
    (pp/print-table [:db/ident :db/valueType :db/cardinality
                     :db/unique :db/doc]
                    ts)
    (-show-enums es)
    (flush)))

(defn show-status [m]
  (let [m' (dissoc m :help)
        ks (-> m' keys sort)
        pks (map name ks)
        k-max-len (->> pks
                       (map (comp count str))
                       (sort >)
                       first)
        row-fmt (str "%-" k-max-len "s : %s\n")]
    (doseq [[k pk] (map vector ks pks)]
      (let [v (get m' k)
            v' (if (instance? Boolean v)
                 (if v "ON" "OFF")
                 v)]
        (printf row-fmt pk v'))))
  (flush))

(defn print-help []
  (println "type `exit` or `quit` or ^D to exit")
  (println "type `debug` to toggle debug mode")
  (println "type `pretend` to toggle pretend mode")
  (println "type `expanded` or `\\x` to toggle expanded display mode")
  (println "type `show tables` or `\\d` to show Datomic \"tables\"")
  (println "type `show schema` or `\\dn` to show all user Datomic schema")
  (println "type `describe $table` or `\\d $table` to describe a Datomic \"table\"")
  (println "type `describe $dbid` or `\\d $dbid` to describe the schema of an entity")
  (println "type `status` to show toggle values, conn strings, etc.")
  (println "type `\\?`, `?`, `h` or `help` to see this listing")
  (flush))

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
        (when (re-seq #"^(?i)\s*(?:\\x|expanded)\s*$" input)
          (let [new-expand (not @x-flag)]
            (println "Set expanded display to" (if new-expand "ON" "OFF"))
            (flush)
            (reset! x-flag new-expand)
            (reset! noop true)))
        (when (re-seq #"^(?i)\s*(?:h|help|\?+|\\\?)\s*$" input)
          (print-help)
          (reset! noop true))
        (when (re-seq #"^(?i)\s*(?:show\s+tables|\\d)\s*$" input)
          (show-tables)
          (reset! noop true))
        (when (re-seq #"^(?i)\s*(?:show\s+schema|\\dn)\s*$" input)
          (show-schema)
          (reset! noop true))
        (when-let [match (re-matches #"^(?i)\s*(?:desc(?:ribe)?|\\d)\s+(\S+)\s*$" input)]
          (let [[_ x] match]
            (if (re-seq #"^\d+$" x)
              (describe-entity (Long/parseLong x))
              (describe-table x))
            (reset! noop true)))
        (when (re-seq #"^(?i)\s*status\s*$" input)
          (show-status opts)
          (reset! noop true))

        (when (and (not @noop) (re-seq #"(?ms)\S" input))
          (let [maybe-ast (par/parser input)]
            (if-not (par/good-ast? maybe-ast)
              (do
                (squawk "Parse error" maybe-ast)
                (when-let [hint (par/hint-for-parse-error maybe-ast)]
                  (binding [*out* *err*]
                    (println (str "\n*** Hint: " hint))))
                (print-ruler input (:column maybe-ast)))
              (do
                (when @dbg (squawk "AST" maybe-ast))
                (let [conn (->> sys :datomic :connection)
                      db (d/db conn)
                      ir (par/transform maybe-ast)
                      opts' {:debug @dbg :pretend @loljk}]
                  (when @dbg (squawk "Intermediate Repr" ir))

                  (case (:type ir)

                    :select
                    (let [result (sel/run-select db ir opts')
                          entities (:entities result)
                          attrs (:attrs result)
                          print-table-fn (if @x-flag
                                           tab/print-expanded-table
                                           tab/print-simple-table)]
                      (print-table-fn (seq attrs) entities)
                      (flush))

                    :update
                    (upd/run-update conn db ir opts')

                    :delete
                    (del/run-delete conn db ir opts')

                    :insert
                    (ins/run-insert conn ir opts')

                    :retract
                    (rtr/run-retract conn db ir opts')

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
                 ["-x" "--expanded"
                  "Display resultsets in expanded output format"
                  :flag true
                  :default false]
                 ["-u" "--connection-uri"
                  "URI to Datomic DB; if missing, uses default mem db"]
                 ["-s" "--default-schema-name"
                  ":dellstore or :starfighter or :seattle, for default in-mem db"
                  :parse-fn (fn [s]
                              (if-let [m (re-matches #"^:+(.+)$" s)]
                                (keyword (second m))
                                (keyword s)))
                  :default :dellstore])]
    (when (:help opts)
      (println banner)
      (System/exit 0))

    (let [opts' (set/rename-keys opts {:default-schema-name :schema-name})]

      (def sys (.start (dat/system opts')))

      (let [uri (->> sys :datomic :connection-uri)]
        (when (dat/default-uri? uri)
          (println "*** using default in-mem database ***"))
        (print-help)
        (repl (assoc opts'
                     :connection-uri uri))))))

(comment

  (def sys (.start (dat/system {})))
  (def conn (->> sys :datomic :connection))
  (def db (d/db conn))

  (let [stmt "select where product.prod-id = 9990"
        ir (->> stmt par/parser par/transform)]
    (->>
     (sel/run-select db ir #_{:debug true})
     :entities
     pp/pprint))

  (let [stmt "update product.rating = 3.14f where product.prod-id > 8000"
        ir (->> stmt par/parser par/transform)]
    (->>
     (upd/run-update conn db ir #_{:debug true :pretend nil})
     pp/pprint)
    (->> (d/q '[:find [?e ...]
                :where
                [?e :product/prod-id ?pid]
                [(> ?pid 8000)]]
              (d/db conn))
         (map #(d/touch (d/entity (d/db conn) %)))
         (map #(into {} %))
         pp/pprint))

  (let [stmt "delete where order.orderid = 2"
        ir (->> stmt par/parser par/transform)]
    (->> (d/q '[:find ?e ?oid
                :where
                [?e :order/orderid ?oid]]
              (d/db conn))
         pp/pprint)
    (->>
     (del/run-delete conn db ir #_{:debug true :pretend nil})
     #_pp/pprint)
    (->> (d/q '[:find ?e ?oid
                :where
                [?e :order/orderid ?oid]]
              (d/db conn))
         pp/pprint))

    (let [stmt "insert customer.customerid = 1234,
                       customer.email = 'foo@example.com'"
        ir (->> stmt par/parser par/transform)]
    (->> (d/q '[:find ?e ?cid
                :where
                [?e :customer/customerid ?cid]]
              (d/db conn))
         pp/pprint)
    (->>
     (ins/run-insert conn ir #_{:debug true :pretend nil})
     pp/pprint)
    (->> (d/q '[:find ?e ?cid
                :where
                [?e :customer/customerid ?cid]]
              (d/db conn))
         pp/pprint))

)
