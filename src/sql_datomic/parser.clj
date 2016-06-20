(ns sql-datomic.parser
  (:require [instaparse.core :as insta]
            [clojure.zip :as zip]
            [clojure.string :as str]
            [clojure.edn :as edn]
            [clj-time.format :as fmt]
            [clj-time.core :as tm]
            [clj-time.coerce :as coer]
            [clojure.instant :as inst]
            [sql-datomic.types :as types]))

(def parser
  (-> "resources/sql-eensy.bnf"
      #_"resources/sql.bnf"
      slurp
      (insta/parser
       :input-format :ebnf
       :no-slurp true
       :string-ci true
       :auto-whitespace :standard)))

(def good-ast? (complement insta/failure?))

(defn strip-doublequotes [s]
  (-> s
      (str/replace #"^\"" "")
      (str/replace #"\"$" "")))

(defn transform-string-literal [s]
  (-> s
      (str/replace #"^'" "")
      (str/replace #"'$" "")
      (str/replace #"\\'" "'")))

(def comparison-ops
  {"=" :=
   "<>" :not=
   "!=" :not=
   "<" :<
   "<=" :<=
   ">" :>
   ">=" :>=})

(def datetime-formatter (fmt/formatters :date-hour-minute-second))
(def date-formatter (fmt/formatters :date))

(defn to-utc [d]
  (tm/from-time-zone d tm/utc))

(defn fix-datetime-separator-space->T [s]
  (str/replace s #"^(.{10}) (.*)$" "$1T$2"))

(defn transform-datetime-literal [s]
  (->> s
       fix-datetime-separator-space->T
       (fmt/parse datetime-formatter)
       to-utc
       str
       inst/read-instant-date))

(defn transform-date-literal [s]
  (->> s
       (fmt/parse date-formatter)
       to-utc
       str
       inst/read-instant-date))

(defn transform-epochal-literal [s]
  (->> s
       Long/parseLong
       (*' 1000)  ;; ->milliseconds
       coer/from-long
       str
       inst/read-instant-date))

(defn transform-float-literal [s]
  (let [s' (if (->> s last #{\F \f})
             (subs s 0 (-> s count dec))
             s)]
   (Float/parseFloat s')))

(def transform-options
  {:sql_data_statement identity
   :select_statement (fn [& ps]
                       (let [ks (case (count ps)
                                  1 [:where]
                                  [:fields :tables :where])]
                         (->> ps
                              (zipmap ks)
                              (into {:type :select}))))
   :update_statement (fn [& ps]
                       (let [ks (case (count ps)
                                  2 [:assign-pairs :where]
                                  [:table :assign-pairs :where])]
                         (->> ps
                              (zipmap ks)
                              (into {:type :update}))))
   :insert_statement (fn [& ps]
                       (let [ks (case (count ps)
                                  1 [:assign-pairs]
                                  [:table :cols :vals])]
                         (->> ps
                              (zipmap ks)
                              (into {:type :insert}))))
   :delete_statement (fn [& ps]
                       (let [ks (case (count ps)
                                  1 [:where]
                                  [:table :where])]
                        (->> ps
                             (zipmap ks)
                             (into {:type :delete}))))
   :select_list vector
   :column_name (fn [t c] {:table (strip-doublequotes t)
                           :column (strip-doublequotes c)})
   :string_literal transform-string-literal
   :long_literal edn/read-string
   :bigint_literal edn/read-string
   :float_literal transform-float-literal
   :double_literal edn/read-string
   :bigdec_literal edn/read-string
   :keyword_literal keyword
   :boolean_literal identity
   :true (constantly true)
   :false (constantly false)
   :from_clause vector
   :table_ref (fn [& vs] (zipmap [:name :alias] vs))
   :where_clause vector
   :table_name strip-doublequotes
   :table_alias strip-doublequotes
   :binary_comparison (fn [c op v]
                        (if (and (= c {:table "db", :column "id"})
                                 (= op "="))
                          (list :db-id v)
                          (list (comparison-ops op) c v)))
   :between_clause (fn [c v1 v2]
                     (list :between c v1 v2))
   :date_literal transform-date-literal
   :datetime_literal transform-datetime-literal
   :epochal_literal transform-epochal-literal
   :inst_literal inst/read-instant-date
   :uuid_literal (fn [s] (java.util.UUID/fromString s))
   :uri_literal types/->uri
   :bytes_literal types/base64-str->bytes
   :set_clausen vector
   :assignment_pair vector
   :insert_cols vector
   :insert_vals vector})

(def transform (partial insta/transform transform-options))

(defn parse [input]
  (->> (parser input)
       transform))

(defn hint-for-parse-error [parse-error]
  (let [{:keys [index reason line column text]} parse-error
        c (get text index)]
    (cond
      (or (= c \")
          (re-seq #"(?i)\bwhere\s+.*[^<>](?:=|!=|<>)\s*\"" text))
      "Did you use \" for string literal?  Strings are delimited by '."

      (and
       (re-seq #"^(?:#bytes|#base64)" (subs text index))
       (re-seq
        #"(?i)\bwhere\s+.*(?:=|!=|<>|<=|<|>=|>)\s*(?:#bytes|#base64)\b"
        text))
      (str "Did you use a #bytes literal in a comparison?  "
           "Bytes arrays are not values"
           " and cannot be used with =, !=, <>, <, etc.")

      :else nil)))
