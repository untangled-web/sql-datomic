(ns sql-datomic.parser
  (:require [instaparse.core :as insta]))

(def parser
  (->> "resources/sql-92.instaparse.bnf"
       slurp
       insta/parser))

(comment

  (parser "SELECT name AS foo FROM a_table")
  (parser "SELECT name FROM a_table")
  (parser "SELECT * FROM a_table")

  )

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
