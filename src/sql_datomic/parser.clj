(ns sql-datomic.parser
  (:require [instaparse.core :as insta]
            [clojure.zip :as zip]
            [clojure.string :as str]
            [clojure.edn :as edn]
            [clj-time.format :as fmt]
            [clj-time.core :as tm]
            [clj-time.coerce :as coer]
            [clojure.instant :as inst]))

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

(defn ->uri [s] (java.net.URI. s))

(defmethod print-dup java.net.URI [uri ^java.io.Writer writer]
  (.write writer (str "#uri \"" (.toString uri) "\"")))

(defmethod print-method java.net.URI [uri ^java.io.Writer writer]
  (print-dup uri writer))

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

;; TODO: Need to add support for these Datomic types, from SQL dialect:
;;
;; :db.type/bytes - Value type for small binary data. Maps to byte
;; array on Java platforms. See limitations.

(def transform-options
  {:sql_data_statement identity
   :select_statement (fn [& ps]
                       (->> ps
                            (zipmap [:fields :tables :where])
                            (into {:type :select})))
   :select_list vector
   :column_name (fn [t c] {:table (strip-doublequotes t)
                           :column (strip-doublequotes c)})
   :string_literal transform-string-literal
   :exact_numeric_literal edn/read-string
   :approximate_numeric_literal edn/read-string
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
                        (list (comparison-ops op) c v))
   :between_clause (fn [c v1 v2]
                     (list :between c v1 v2))
   :date_literal transform-date-literal
   :datetime_literal transform-datetime-literal
   :epochal_literal transform-epochal-literal
   :inst_literal inst/read-instant-date
   :uuid_literal (fn [s] (java.util.UUID/fromString s))
   :uri_literal ->uri})

(def transform (partial insta/transform transform-options))

(defn parse [input]
  (->> (parser input)
       transform))
