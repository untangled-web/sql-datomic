(ns sql-datomic.parser-test
  (:require [clojure.test :refer :all]
            [sql-datomic.parser :as prs]
            [instaparse.core :as insta]
            [clj-time.core :as tm]
            [clojure.instant :as inst]))

(defmacro parsable? [stmt]
  `(is (prs/good-ast? (prs/parser ~stmt))))

(deftest parser-tests
  (testing "SELECT statements"
    (parsable? "SELECT a_table.name FROM a_table")
    (parsable? "SELECT a_table.name FROM a_table")
    (parsable? "SELECT a_table.name, a_table.age FROM a_table")
    (parsable? "SELECT a_table.* FROM a_table")
    (parsable? "SELECT a_table.name FROM a_table, b_table")
    (parsable?
     "SELECT a_table.foo
      FROM   a_table
     ")
    (parsable?
     "SELECT a_table.foo,
		a_table.bar,
             a_table.baz
      FROM   a_table
     ")
    (parsable?
     "
         SELECT a_table.foo FROM   a_table
     ")
    (parsable? "SELECT a_table.name FROM a_table WHERE a_table.name = 'foo'")
    (parsable?
     "SELECT a_table.*, b_table.zebra_id
      FROM a_table
         , b_table
      WHERE a_table.id = b_table.a_id
        AND b_table.zebra_id > 9000")
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
      WHERE a_table.foo <> 0")
    (parsable?
     "SELECT a_table.*, b_table.zebra_id
      FROM a_table
         , b_table
      WHERE a_table.id = b_table.a_id
        AND b_table.zebra_id != 0
      ")
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
      ")
    (parsable?
     "SELECT a_table.*, b_table.zebra_id
      FROM a_table, b_table
      WHERE  a_table.id = b_table.a_id
        AND  b_table.zebra_id > 9000")
    (parsable?
     "SELECT a_table.*
      FROM a_table
      WHERE a_table.created_on BETWEEN DATE '2007-02-01'
                                   AND DATE '2010-10-10'")
    (parsable?
     "select foo.name
        from foo
       where foo.title = 'git-\\'r-dun maestro'
         and foo.hired_on <= date '2000-01-01'")
    (parsable?
     "select foo.id, foo.name, foo.title
        from foo
       where foo.is_active = true
         and foo.group_id >= 3")
    (parsable?
     "select survey-request.email,
             survey-request.completion-status
        from survey-request
       where survey-request.sent-date between date '2014-04-01'
                                          and date '2014-05-01'
     ")
    (parsable?
     "SELECT a_table.*
      FROM a_table
      WHERE a_table.created_on BETWEEN DATETIME '2007-02-01T10:11:12'
                               AND #inst \"2010-10-10T01:02:03.001-07:00\"")
    (parsable?
     "select foo.* from foo
      where product.prod-id between 4000 and 6000 and
            product.category <> :product.category/action"))

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
      ('foo', 42, 1234.56, date '2016-04-01')"))

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
        and  employees.hired_on < date '2010-01-01'"))

  (testing "DELETE statements"
    (parsable?
     "delete from products where products.actor = 'homer simpson'")
    (parsable?
     "delete from drop_bear_attacks")
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
     ")))

(deftest transform-tests
  (testing "SELECT AST -> IR"
    (is (= (prs/transform
            [:sql_data_statement
             [:select_statement
              [:select_list
               [:qualified_asterisk "a_table"]
               [:column_name "b_table" "zebra_id"]]
              [:from_clause
               [:table_ref [:table_name "a_table"]]
               [:table_ref [:table_name "b_table"]]]
              [:where_clause
               [:binary_comparison
                [:column_name "a_table" "id"]
                "="
                [:column_name "b_table" "a_id"]]
               [:binary_comparison
                [:column_name "b_table" "zebra_id"]
                ">"
                [:exact_numeric_literal "9000"]]
               [:binary_comparison
                [:column_name "b_table" "hired_on"]
                "<"
                [:date_literal "2011-11-11"]]]]])
           {:type :select
            :fields [[:qualified_asterisk "a_table"]
                     {:table "b_table" :column "zebra_id"}]
            :tables [{:name "a_table"} {:name "b_table"}]
            :where [(list :=
                          {:table "a_table" :column "id"}
                          {:table "b_table" :column "a_id"})
                    (list :>
                          {:table "b_table" :column "zebra_id"}
                          9000)
                    (list :<
                          {:table "b_table" :column "hired_on"}
                          (->> (tm/date-time 2011 11 11)
                               str
                               inst/read-instant-date))]
            }))
    ;; select product.prod-id from product where product.prod-id between 1 and 2 and product.title <> 'foo'
    (is (= (prs/transform
            [:sql_data_statement
             [:select_statement
              [:select_list [:column_name "product" "prod-id"]]
              [:from_clause [:table_ref [:table_name "product"]]]
              [:where_clause
               [:between_clause
                [:column_name "product" "prod-id"]
                [:exact_numeric_literal "1"]
                [:exact_numeric_literal "10"]]
               [:binary_comparison
                [:column_name "product" "title"]
                "<>"
                [:string_literal "'foo'"]]]]])
           {:type :select,
            :fields [{:table "product", :column "prod-id"}],
            :tables [{:name "product"}],
            :where
            [(list :between {:table "product", :column "prod-id"} 1 10)
             (list :not= {:table "product", :column "title"} "foo")]}
           ))
    ;; select foo.* from foo where product.prod-id between 4000 and 6000 and product.category <> :product.category/action
    (is (= (prs/transform
            [:sql_data_statement
             [:select_statement
              [:select_list [:qualified_asterisk "foo"]]
              [:from_clause [:table_ref [:table_name "foo"]]]
              [:where_clause
               [:between_clause
                [:column_name "product" "prod-id"]
                [:exact_numeric_literal "4000"]
                [:exact_numeric_literal "6000"]]
               [:binary_comparison
                [:column_name "product" "category"]
                "<>"
                [:keyword_literal "product.category/action"]]]]])
           {:type :select,
            :fields [[:qualified_asterisk "foo"]],
            :tables [{:name "foo"}],
            :where
            ['(:between {:table "product", :column "prod-id"} 4000 6000)
             '(:not=
              {:table "product", :column "category"}
              :product.category/action)]}))))
