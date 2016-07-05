(ns sql-datomic.integration-test
  (:require [clojure.test :refer :all]
            [sql-datomic.datomic :as dat]
            sql-datomic.types
            [sql-datomic.parser :as par]
            [sql-datomic.select-command :as sel]
            [datomic.api :as d]))

(def ^:dynamic *conn* :not-a-connection)

(def ^:dynamic *db* :not-a-db)

(defn current-db [] (d/db *conn*))

(defn entity->map
  ([eid] (entity->map *db* eid))
  ([db eid]
   (if-let [e (d/entity db eid)]
     (->> e d/touch (into {:db/id (:db/id e)}))
     {})))

(defn product-map->comparable [m]
  ;; cannot compare byte arrays and :db/id changes per db refresh
  (dissoc m :db/id :product/blob))

(defn -select-keys [ks m]
  (select-keys m ks))

(defn db-fixture [f]
  (let [sys (.start (dat/system {}))]
    #_(do (println "=== db set-up") (flush))
    (binding [*conn* (->> sys :datomic :connection)]
      (binding [*db* (d/db *conn*)]
        (f)))
    (.stop sys)
    #_(do (println "=== db tear-down") (flush))))

(use-fixtures :each db-fixture)

(deftest product-entity-present
  (is (= (product-map->comparable (entity->map *db* [:product/prod-id 8293]))
         {:product/url #uri "http://example.com/products/8293"
          :product/prod-id 8293
          :product/uuid #uuid "57607472-0bd8-4ed3-98d3-586e9e9c9683"
          :product/common-prod-id 4804
          :product/man-hours 100N
          :product/price 13.99M
          :product/category :product.category/family
          :product/tag :alabama-exorcist-family
          :product/actor "NICK MINELLI"
          :product/rating #float 2.6
          :product/special false
          :product/title "ALABAMA EXORCIST"})))

(defn select-stmt->ir [stmt]
  (->> stmt par/parser par/transform))

(deftest select-product-by-prod-id
  (let [ir (select-stmt->ir "select where product.prod-id = 9990")]
    (is (= (->> (sel/run-select *db* ir)
                :entities
                (map product-map->comparable))
           [{:product/url #uri "http://example.com/products/9990"
             :product/prod-id 9990
             :product/uuid #uuid "57607426-cdd4-49fa-aecb-0a2572976db9"
             :product/common-prod-id 6584
             :product/man-hours 60100200300N
             :product/price 25.99M
             :product/category :product.category/music
             :product/tag :aladdin-world-music
             :product/actor "HUMPHREY DENCH"
             :product/rating #float 2.0
             :product/special false
             :product/title "ALADDIN WORLD"}]))))

(deftest select-all-products-by-prod-id
  (let [ir (select-stmt->ir "select where product.prod-id > 0")
        prod-ids #{1298 1567
                   2290 2926
                   4402 4936
                   5130
                   6127 6376 6879
                   8293
                   9990}]
    (is (= (->> (sel/run-select *db* ir)
                :entities
                (map :product/prod-id)
                (into #{}))
           prod-ids))))

(deftest select-prod-id-6k-products
  (let [ir (select-stmt->ir
            "select where product.prod-id between 6000 and 6999")
        prod-ids #{6127 6376 6879}]
    (is (= (->> (sel/run-select *db* ir)
                :entities
                (map :product/prod-id)
                (into #{}))
           prod-ids))))

(deftest select-no-products-by-prod-id
  (let [ir (select-stmt->ir
            "select where product.prod-id >= 10000")
        prod-ids #{}]
    (is (= (->> (sel/run-select *db* ir)
                :entities
                (map :product/prod-id)
                (into #{}))
           prod-ids))))

(deftest select-order-between-dates
  (let [ir (select-stmt->ir
            "select where
             order.orderdate between #inst \"2004-01-01\"
                                 and #inst \"2004-01-05\" ")
        expected #{{:order/orderid 2
                    :order/orderdate #inst "2004-01-01T08:00:00.000-00:00"}}]
    (is (= (->> (sel/run-select *db* ir)
                :entities
                (map (partial -select-keys
                              [:order/orderid :order/orderdate]))
                (into #{}))
           expected))))

(comment

  (defn pp-ent [eid]
    (db-fixture
     (fn []
       (->> (entity->map *db* eid)
            clojure.pprint/pprint))))
  (pp-ent [:product/prod-id 9990])

  )

#_(deftest parser-tests
  (testing "SELECT statements"
    (parsable? "SELECT a_table.name FROM a_table"
               "allows simple queries")
    (parsable? "SELECT a_table.name, a_table.age FROM a_table"
               "allows multiple columns in select list")
    (parsable? "SELECT a_table.* FROM a_table"
               "allows qualified star in select list")
    (parsable? "SELECT a_table.name FROM a_table, b_table"
               "allows implicit cartesian product in from list")
    (parsable?
     "SELECT a_table.foo
      FROM   a_table
     " "tolerant of newlines")
    (parsable?
     "SELECT a_table.foo,\r
		a_table.bar,\r
             a_table.baz\r
      FROM   a_table\r
     " "tolerant of tabs and carriage returns")
    (parsable?
     "
         SELECT a_table.foo FROM   a_table
     " "tolerant of newlines at beginning and end")
    (parsable? "SELECT a_table.name FROM a_table WHERE a_table.name = 'foo'"
               "allows string literals")
    (parsable?
     "SELECT a_table.*, b_table.zebra_id
      FROM a_table
         , b_table
      WHERE a_table.id = b_table.a_id
        AND b_table.zebra_id > 9000"
     "supports where clause conjoined by and")
    (parsable?
     "SELECT a_table.*, b_table.zebra_id
      FROM a_table, b_table
      WHERE a_table.id = b_table.a_id
      ")
    (parsable?
     "SELECT a_table.*, b_table.zebra_id
      FROM a_table, b_table
      WHERE a_table.id = b_table.a_id
        AND b_table.zebra_id > 9000
      ")
    (parsable?
     "SELECT a_table.*
      FROM a_table
      WHERE a_table.foo <> 0"
     "supports <> as binary comparison in where clauses")
    (parsable?
     "SELECT a_table.*, b_table.zebra_id
      FROM a_table
         , b_table
      WHERE a_table.id = b_table.a_id
        AND b_table.zebra_id != 0
      " "supports != as binary comparison in where clauses")
    (parsable?
     "select
                        a_table.*
                      , b_table.zebra_id
      from
                        a_table
                      , b_table
      where
                        a_table.id = b_table.a_id
                 and    b_table.zebra_id > 0
      " "tolerant of lowercase res words and exotic whitespace formatting")
    (parsable?
     "SELECT a_table.*, b_table.zebra_id
      FROM a_table, b_table
      WHERE  a_table.id = b_table.a_id
        AND  b_table.zebra_id > 9000")
    (parsable?
     "SELECT a_table.*
      FROM a_table
      WHERE a_table.created_on BETWEEN DATE '2007-02-01'
                                   AND DATE '2010-10-10'"
     "supports between with date literals")
    (parsable?
     "select foo.name
        from foo
       where foo.title = 'git-\\'r-dun maestro'
         and foo.hired_on <= date '2000-01-01'"
     "allows string literals with embedded single quote")
    (parsable?
     "select foo.id, foo.name, foo.title
        from foo
       where foo.is_active = true
         and foo.group_id >= 3"
     "supports boolean literals in where clause")
    (parsable?
     "select survey-request.email,
             survey-request.completion-status
        from survey-request
       where survey-request.sent-date between date '2014-04-01'
                                          and date '2014-05-01'
     "
     "supports hyphenation in table/column identifiers")
    (parsable?
     "SELECT a_table.*
      FROM a_table
      WHERE a_table.created_on BETWEEN DATETIME '2007-02-01T10:11:12'
                               AND #inst \"2010-10-10T01:02:03.001-07:00\""
     "supports #inst literals used alongside datetime literal")
    (parsable?
     "select foo.* from foo
      where product.prod-id between 4000 and 6000 and
            product.category <> :product.category/action"
     "supports namespaced keyword literals as value in where clause")
    (parsable?
     "select foo.bar from foo where product.tag = :alabama-exorcist-family"
     "supports non-namespaced keyword literals as value in where")
    (parsable?
     "select foo.bar from foo where
        product.uuid between
              #uuid \"576073c3-24c5-4461-9b84-dfe65774d41b\"
          and #uuid \"5760745a-5bb5-4768-96f7-0f8aeb1a84f0\""
     "supports #uuid literals")
    (parsable?
     "select foo.bar from foo where
        product.url = #uri \"http://example.com/products/2290\""
     "supports #uri literals")
    (parsable?
     "select foo.bar, #bytes \"QURBUFRBVElPTiBVTlRPVUNIQUJMRVM=\"
      from foo where product.prod-id between 2000 and 3000"
     "supports #bytes (byte array) literals in select list")
    (parsable?
     "select foo.bar from foo where
          #attr :product/url = #uri \"http://example.com/products/2290\"
      and #attr :product/category = :product.category/documentary"
     "supports namespaced keyword as column (attr) in where clause")
    (is (= (prs/parser
            "select #attr :foo/bar from foo
              where #attr :foo/quux = :xyzzy.baz/foo")
           [:sql_data_statement
            [:select_statement
             [:select_list [:column_name "foo" "bar"]]
             [:from_clause [:table_ref [:table_name "foo"]]]
             [:where_clause
              [:search_condition
               [:boolean_term
                [:boolean_factor
                 [:boolean_test
                  [:binary_comparison
                   [:column_name "foo" "quux"]
                   "="
                   [:keyword_literal "xyzzy.baz/foo"]]]]]]]]])
        "ns-keyword in select list maps to column_name")
    (parsable? "select foo.bar from product
                where #attr :db/id = 17592186045445"
               "supports :db/id")
    (is (= (prs/parser
            "select 42, 1234N, -12, -69N, 3.14159, 6.626E34, 1e-2, 2.7182M,
                    1.6182F, #float 99.99999
             from foobar")
           [:sql_data_statement
            [:select_statement
             [:select_list
              [:long_literal "42"]
              [:bigint_literal "1234N"]
              [:long_literal "-12"]
              [:bigint_literal "-69N"]
              [:double_literal "3.14159"]
              [:double_literal "6.626E34"]
              [:double_literal "1e-2"]
              [:bigdec_literal "2.7182M"]
              [:float_literal "1.6182F"]
              [:float_literal "99.99999"]]
             [:from_clause [:table_ref [:table_name "foobar"]]]]])
        "supports long, float, double, bigint, bigdec literals")
    (parsable? "select where #attr :product/prod-id between 1567 and 6000"
               "allow shortened where-only select statement")
    (parsable?
     "select #attr :product/title
      from product
      where #attr :product/actor in (
        'GENE WILLIS', 'RIP DOUGLAS', 'KIM RYDER')"
     "supports IN clauses")
    (parsable?
     "select where
             #attr :product/rating > 2.5f
          or (#attr :product/category = :product.category/new
              and #attr :product/prod-id < 5000)
         and #attr :product/price > 22.0M
          or #attr :product/prod-id between 6000 and 7000"
     "supports nested AND-OR trees in WHERE")
    (parsable?
     "select where
              #attr :product/rating > 2.5f
           or (#attr :product/category = :product.category/new
               or (not (
                         (#attr :product/prod-id < 5000
                           or (not #attr :product/category in (
                                   :product.category/action,
                                   :product.category/comedy
                               ))
                           or #attr :product/price > 20.0M
                        and #attr :product/prod-id >= 2000))))"
     "supports nested AND-OR-NOT trees in WHERE"))

  (testing "INSERT statements"
    (parsable?
     "insert into customers (
          firstname, lastname, address1, address2,
          city, state, zip, country
      ) values (
          'Foo', 'Bar', '123 Some Place', '',
          'Thousand Oaks', 'CA', '91362', 'USA'
      )
      ")
    (parsable?
     "INSERT INTO
        foo
      (name, age, balance, joined_on)
      VALUES
      ('foo', 42, 1234.56, date '2016-04-01')"
     "allows string, integral, float, and date literals as values")
    (parsable?
     "insert into
        #attr :product/prod-id = 9999,
        #attr :product/actor = 'Naomi Watts',
        #attr :product/title = 'The Ring',
        #attr :product/category = :product.category/horror,
        #attr :product/rating = 4.5f,
        #attr :product/man-hours = 9001N,
        #attr :product/price = 21.99M")
    "allows shortened assignment-pairs version of insert")

  (testing "UPDATE statements"
    (parsable?
     "update      customers
      set         customers.city = 'Springfield'
                , customers.state = 'VA'
                , customers.zip = '22150'
      where       customers.id = 123454321")
    (parsable?
     "update employees
      set    employees.salary = 40000.00
      where  employees.salary < 38000.00
        and  employees.hired_on < date '2010-01-01'")
    (parsable?
     "update #attr :product/rating = 3.5f
       where #attr :product/prod-id = 1567"
     "allow SET and table to be optional; requires WHERE")
    (parsable?
     "update set #attr :product/rating = 2.4F
       where #attr :product/prod-id = 1567"
     "allow table to be optional; requires WHERE"))

  (testing "DELETE statements"
    (parsable?
     "delete from products where products.actor = 'homer simpson'")
    (parsable?
     "delete from drop_bear_attacks"
     "allows where clause to be optional")
    (parsable?
     "
     DELETE FROM
         lineitems
     WHERE
         lineitems.user_id = 42
     AND lineitems.created_at BETWEEN
         DATETIME '2013-11-15T00:00:00'
           AND
         DATETIME '2014-01-15T08:00:00'
     ")
    (parsable? "delete where #attr :product/prod-id = 1567"
               "allow shortened where-only form"))

  (testing "RETRACT statements"
    (parsable? "RETRACT product.uuid WHERE db.id = 12345")
    (parsable?
     "retract #attr :product/actor,
              #attr :product/rating,
              #attr :product/url
      where db.id 12345 54321 42")
    (parsable?
     "retract #attr :customer/email,
              #attr :customer/zip,
              #attr :customer/firstname
      where db.id in (11111 22222 33333)")))
