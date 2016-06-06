(ns sql-datomic.parser
  (:require [instaparse.core :as insta]
            [clojure.zip :as zip]
            [clojure.string :as str]))

(def parser
  (-> "resources/sql-basic.bnf"
      #_"resources/sql.bnf"
      slurp
      (insta/parser
       :input-format :ebnf
       :no-slurp true
       :string-ci true
       :auto-whitespace :standard)))

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

(declare simplify-select
         simplify-delete
         simplify-update
         simplify-insert)

(defn simplify-ast [ast]
  {:pre [(vector? ast)
         (seq ast)
         (= :sql_data_statement (get-in ast [0]))
         (#{:select_statement
            :delete_statement
            :insert_statement
            :update_statement} (get-in ast [1 0]))]}
  (let [statement_type (get-in ast [1 0])
        root (get-in ast [1 1])]
    (case statement_type
      :select_statement (simplify-select root)
      :delete_statement (simplify-delete root)
      :insert_statement (simplify-insert root)
      :update_statement (simplify-update root)
      (ex-info "unknown statement type" {:statement_type statement_type
                                         :ast ast}))))

(defn descend-to [match-fn node]
  (loop [n node]
    (if (zip/end? n)
      nil
      (let [v (zip/node n)]
        (if (match-fn v)
          v
          (recur (zip/next n)))))))

(defn start-of-select? [node]
  (and (vector? node)
       (= (first node) :query_specification)))

(defn descend-to-value-expression [node]
  (descend-to (fn [n]
                (and (vector? n)
                     (= (first n) :value_expression_primary)))
              node))

(defn field-value [value-expr-node]
  {:pre [(= (first value-expr-node) :value_expression_primary)]}
  (let [key (get-in value-expr-node [1 0])]
    (case key
      :unsigned_value_specification :string-or-int-lit
      :column_reference :column-name
      (ex-info "unknown field" {:field key
                                :node value-expr-node}))))

(defn extract-string-literal [node]
  (when-let [n (descend-to (fn [v]
                             (and (vector? v)
                                  (= (first v) :character_string_literal)))
                           node)]
    (:character_string_literal n)))

(defn extract-identifier-body [node]
  (when-let [n (descend-to (fn [v]
                             (and (vector? v)
                                  (= (first v) :identifier_body)))
                           node)]
    (:identifier_body n)))

(defn extract-digits [node]
  (when-let [n (descend-to (fn [v]
                             (and (vector? v)
                                  (= (first v) :identifier_body)))
                           node)]
    (:identifier_body n)))

(defn field-value-column-name [node]
  (let [col (descend-to (fn [v]
                       (and (vector? v)
                            (= (first v) :column_name)))
                        node)]
    ))

(defn select-fields [select-node]
  (into [] (remove #(= [:comma ","] %) select-node)))

(defn simplify-select [ast]
  (let [z (zip/vector-zip ast)
        s (descend-to start-of-select? z)])
  :select)
(defn simplify-delete [ast]
  :delete)
(defn simplify-update [ast]
  :update)
(defn simplify-insert [ast]
  :insert)

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
