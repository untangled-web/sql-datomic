(defproject sql-datomic "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [instaparse "1.4.2"]
                 [clj-time "0.12.0"]
                 [com.datomic/datomic-pro "0.9.5206" :exclusions [joda-time]]
                 [com.stuartsierra/component "0.3.1"]]
  :main sql-datomic.repl)
