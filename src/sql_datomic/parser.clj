(ns sql-datomic.parser
  (:require [instaparse.core :as insta]))

(def parser
  (-> #_"resources/sql-92.instaparse.bnf"
      "resources/sql.bnf"
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
  (parser
   "update      customers
    set         city = 'Springfield'
              , state = 'VA'
              , zip = '22150'
    where       id = 123454321")
  (parser
   "delete from products where actor = 'homer simpson'")

  )

(comment

  ;; Such AST ...
  ;; Do not want.  Trim please.
  (->> "delete from products where actor = 'homer simpson'"
       parser
       clojure.pprint/pprint)
  [:direct_SQL_statement
   [:direct_SQL_data_statement
    [:delete_statement_searched
     "DELETE"
     "FROM"
     [:table_name
      [:qualified_name
       [:qualified_identifier
        [:identifier
         [:actual_identifier
          [:regular_identifier [:identifier_body "products"]]]]]]]
     "WHERE"
     [:search_condition
      [:boolean_term
       [:boolean_factor
        [:boolean_test
         [:boolean_primary
          [:predicate
           [:comparison_predicate
            [:row_value_constructor
             [:row_value_constructor_element
              [:value_expression
               [:interval_value_expression
                [:interval_term
                 [:interval_factor
                  [:interval_primary
                   [:value_expression_primary
                    [:column_reference
                     [:column_name
                      [:identifier
                       [:actual_identifier
                        [:regular_identifier
                         [:identifier_body "actor"]]]]]]]]]]]]]]
            [:comp_op [:equals_operator "="]]
            [:row_value_constructor
             [:row_value_constructor_element
              [:value_expression
               [:interval_value_expression
                [:interval_term
                 [:interval_factor
                  [:interval_primary
                   [:value_expression_primary
                    [:unsigned_value_specification
                     [:unsigned_literal
                      [:general_literal
                       [:character_string_literal
                        "homer simpson"]]]]]]]]]]]]]]]]]]]]]]

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
