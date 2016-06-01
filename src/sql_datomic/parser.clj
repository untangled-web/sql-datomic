(ns sql-datomic.parser
  (:require [instaparse.core :as insta]))

(def parser
  (->> #_"resources/sql.bnf" #_"resources/sql-92.instaparse.bnf"
       "resources/sql-92.no-hiddens.instaparse.bnf"
       slurp
       insta/parser))

;; (def transform-operator
;;   {"+" +
;;    "-" -
;;    "*" *
;;    "/" /})

;; (defn transform-operation [f v]
;;   (if (coll? v)
;;     (apply f v)
;;     (f v)))

;; (def transform-options
;;   {:expr identity
;;    :vector vector
;;    :number read-string
;;    :operator transform-operator
;;    :operation transform-operation})

;; (defn parse [input]
;;   (->> (parser input)
;;        (insta/transform transform-options)))
