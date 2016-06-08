(ns sql-datomic.parser
  (:require [instaparse.core :as insta]
            [clojure.zip :as zip]
            [clojure.string :as str]))

(def parser
  (-> "resources/sql-eensy.bnf"
      #_"resources/sql.bnf"
      slurp
      (insta/parser
       :input-format :ebnf
       :no-slurp true
       :string-ci true
       :auto-whitespace :standard)))

(defn column-name-ast? [ast]
  (and (vector? ast) (= (first ast) :column_name)))

(defn column-name-ast->ir [ast]
  {:pre [(vector? ast)
         (= 3 (count ast))
         (= (first ast) :column_name)
         (string? (nth ast 1))
         (string? (nth ast 2))]}
  (let [[_ table-name column-name] ast]
    {:table table-name, :column column-name}))

(defn where-clause-ast->ir [ast]
  {:pre [(vector? ast)
         (= (first ast) :where_clause)]}
  (rest ast))

(defn table-ref-ast->ir [ast]
  {:pre [(vector? ast)
         (= (first ast) :table_ref)
         (vector? (second ast))
         (= (get-in ast [1 0]) :table_name)
         (string? (get-in ast [1 1]))]}
  (when (>= (count ast) 3)
    (assert (vector? (get-in ast [2])) "invalid table_alias")
    (assert (= (get-in ast [2 0]) :table_alias) "invalid table_alias")
    (assert (string? (get-in ast [2 1])) "invalid table_alias"))
  (let [[_ [_ table-name] maybe-table-alias] ast
        result {:name table-name}]
    (if maybe-table-alias
      (assoc result :alias (second maybe-table-alias))
      result)))

(defn from-clause-ast->ir [ast]
  {:pre [(vector? ast)
         (= (first ast) :from_clause)]}
  (->> (rest ast)
       (map table-ref-ast->ir)))

(defn select-list-ast->ir [ast]
  {:pre [(vector? ast)
         (= (first ast) :select_list)]}
  (->> (rest ast)
       (map (fn [v]
              (if (column-name-ast? v)
                (column-name-ast->ir v)
                v)))))

(defn select-statement-ast->ir [ast]
  {:pre [(vector? ast)
         (>= (count ast) 3)
         (= (first ast) :select_statement)]}
  (let [[_ select-list from-clause where-clause] ast
        result {:tables (from-clause-ast->ir from-clause)
                :fields (select-list-ast->ir select-list)}]
    (if where-clause
      (assoc result :where (where-clause-ast->ir where-clause))
      result)))

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
