(ns sql-datomic.parser
  (:require [instaparse.core :as insta]))

(def parser
  (-> "resources/sql-92.instaparse.bnf"
      slurp
      (insta/parser
       :input-format :ebnf
       :no-slurp true
       :string-ci true
       :auto-whitespace :standard)))

(comment

  (parser "SELECT name AS foo FROM a_table")
  (parser "SELECT name FROM a_table")
  (parser "SELECT name, age AS bleh FROM a_table")
  (parser "SELECT * FROM a_table")
  (parser "SELECT name FROM a_table, b_table")
  (parser "SELECT name FROM a_table WHERE name = 'foo'")
  (parser
     "SELECT foo AS \"fuu\"
      FROM   a_table
     ")
  (parser
   "SELECT a.*, b.zebra_id
      FROM a_table a
      LEFT OUTER JOIN b_table b ON a.id = b.a_id
      WHERE b.zebra_id > 9000
      ")
  (parser
   "SELECT *
      FROM a_table a
      WHERE a.foo IS NOT NULL
      ")
  (parser
   "SELECT a.*, b.zebra_id
      FROM a_table a
      LEFT OUTER JOIN b_table b ON a.id = b.a_id
      WHERE b.zebra_id IS NOT NULL
      ")
  (parser
   "select a.*, b.zebra_id
      from a_table a
      left outer join b_table b on a.id = b.a_id
      where b.zebra_id is not null
      ")
  (parser
   "SELECT a.*, b.zebra_id
      FROM a_table a
      INNER JOIN b_table b ON a.id = b.a_id
      WHERE b.zebra_id > 9000")
  (parser
   "SELECT *
      FROM a_table a
      WHERE a.created_on BETWEEN DATE '2007-02-01'
                             AND DATE '2010-10-10'")
  (parser
   "insert into customers (
        firstname, lastname, address1, address2,
        city, state, zip, country
    ) values (
        'Foo', 'Bar', '123 Some Place', '',
        'Thousand Oaks', 'CA', '91362', 'USA'
    )
    ")
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
