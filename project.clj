(defproject sql-datomic "0.1.0"
  :description "Interpreter of a SQL-ish dialect that runs against Datomic databases."
  :url "https://github.com/untangled-web/sql-datomic"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [instaparse "1.4.2"]
                 [clj-time "0.12.0"]
                 #_[com.datomic/datomic-pro "0.9.5206" :exclusions [joda-time]]
                 [com.datomic/datomic-free "0.9.5344" :scope "provided" :exclusions [joda-time]]
                 [com.stuartsierra/component "0.3.1"]
                 [org.clojure/tools.cli "0.3.5"]
                 [org.postgresql/postgresql "9.4.1208.jre7"]
                 [com.datastax.cassandra/cassandra-driver-core "2.0.6"
                  :exclusions [com.google.guava/guava
                               org.slf4j/slf4j-api]]]
  :main sql-datomic.repl
  ;; :aot [sql-datomic.repl]
  )
