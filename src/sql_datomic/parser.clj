(ns sql-datomic.parser
  (:require [instaparse.core :as insta]))

(def parser
  (->> "resources/sql.bnf"
       slurp
       insta/parser))
