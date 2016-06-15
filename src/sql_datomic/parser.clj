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

(defn string->bytes [^String s]
  (.getBytes s java.nio.charset.StandardCharsets/ISO_8859_1))
(defn bytes->string [#^bytes b]
  (String. b java.nio.charset.StandardCharsets/ISO_8859_1))
(defn bytes->base64-str [#^bytes b]
  (.encodeToString (java.util.Base64/getEncoder) b))
(defn base64-str->bytes [^String b64str]
  (.decode (java.util.Base64/getDecoder) b64str))

(defonce bytes-class (Class/forName "[B"))
(defmethod print-dup bytes-class [#^bytes b, ^java.io.Writer writer]
  (.write writer (str "#bytes \"" (bytes->base64-str b) "\"")))
(defmethod print-method bytes-class [#^bytes b, ^java.io.Writer writer]
  (print-dup b writer))

;; http://docs.datomic.com/schema.html#bytes-limitations
;; <quote>
;; The bytes :db.type/bytes type maps directly to Java byte arrays,
;; which do not have value semantics (semantically equal byte arrays
;; do not compare or hash as equal).
;; </quote>
(defn bytes= [& bs]
  (apply = (map seq bs)))



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
   :uri_literal ->uri
   :bytes_literal base64-str->bytes})

(def transform (partial insta/transform transform-options))

(defn parse [input]
  (->> (parser input)
       transform))
