(ns sql-datomic.parser-test
  (:require [clojure.test :refer :all]
            [sql-datomic.parser :as prs]
            [instaparse.core :as insta]
            [clj-time.core :as tm]
            [clojure.instant :as inst]))

(defmacro parsable?
  ([stmt]     `(is (prs/good-ast? (prs/parser ~stmt))))
  ([stmt msg] `(is (prs/good-ast? (prs/parser ~stmt)) ~msg)))

(deftest parser-tests
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
              :product/url = #uri \"http://example.com/products/2290\"
          and :product/category = :product.category/documentary"
     "supports namespaced keyword as column (attr) in where clause")
    (is (= (prs/parser
            "select :foo/bar from foo where :foo/quux = :xyzzy.baz/foo")
           [:sql_data_statement
            [:select_statement
             [:select_list [:column_name "foo" "bar"]]
             [:from_clause [:table_ref [:table_name "foo"]]]
             [:where_clause
              [:binary_comparison
               [:column_name "foo" "quux"]
               "="
               [:keyword_literal "xyzzy.baz/foo"]]]]])
        "ns-keyword in select list maps to column_name")
    (parsable? "select foo.bar from product where :db/id = 17592186045445"
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
        "supports long, float, double, bigint, bigdec literals"))

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
     "allows string, integral, float, and date literals as values"))

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
                [:long_literal "9000"]]
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
                [:long_literal "1"]
                [:long_literal "10"]]
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
                [:long_literal "4000"]
                [:long_literal "6000"]]
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
              :product.category/action)]}))

    ;; select foo.bar from foo where product.tag = :alabama-exorcist-family
    (is (prs/transform
         [:sql_data_statement
           [:select_statement
            [:select_list
             [:column_name "foo" "bar"]]
            [:from_clause [:table_ref [:table_name "foo"]]]
            [:where_clause
             [:binary_comparison
              [:column_name "product" "tag"]
              "="
              [:keyword_literal "alabama-exorcist-family"]]]]])
        {:type :select,
         :fields
         [{:table "foo", :column "bar"}],
         :tables [{:name "foo"}],
         :where
         ['(:= {:table "product", :column "tag"} :alabama-exorcist-family)]})

    ;; select foo.bar from foo where product.uuid between #uuid "576073c3-24c5-4461-9b84-dfe65774d41b" and #uuid "5760745a-5bb5-4768-96f7-0f8aeb1a84f0"
    (is (prs/transform
         [:sql_data_statement
          [:select_statement
           [:select_list [:column_name "foo" "bar"]]
           [:from_clause [:table_ref [:table_name "foo"]]]
           [:where_clause
            [:between_clause
             [:column_name "product" "uuid"]
             [:uuid_literal "576073c3-24c5-4461-9b84-dfe65774d41b"]
             [:uuid_literal "5760745a-5bb5-4768-96f7-0f8aeb1a84f0"]]]]])
        {:type :select,
         :fields [{:table "foo", :column "bar"}],
         :tables [{:name "foo"}],
         :where
         ['(:between
           {:table "product", :column "uuid"}
           #uuid "576073c3-24c5-4461-9b84-dfe65774d41b"
           #uuid "5760745a-5bb5-4768-96f7-0f8aeb1a84f0")]})

    ;; select foo.bar from foo where product.url = #uri "http://example.com/products/2290"
    (is (prs/transform
         [:sql_data_statement
          [:select_statement
           [:select_list [:column_name "foo" "bar"]]
           [:from_clause [:table_ref [:table_name "foo"]]]
           [:where_clause
            [:binary_comparison
             [:column_name "product" "url"]
             "="
             [:uri_literal "http://example.com/products/2290"]]]]])
        {:type :select,
         :fields [{:table "foo", :column "bar"}],
         :tables [{:name "foo"}],
         :where
         ['(:=
            {:table "product", :column "url"}
            #uri "http://example.com/products/2290")]})

    ;; select foo.bar, #bytes "QURBUFRBVElPTiBVTlRPVUNIQUJMRVM=" from foo where product.prod-id between 2000 and 3000
    (is (prs/transform
         [:sql_data_statement
          [:select_statement
           [:select_list
            [:column_name "foo" "bar"]
            [:bytes_literal "QURBUFRBVElPTiBVTlRPVUNIQUJMRVM="]]
           [:from_clause [:table_ref [:table_name "foo"]]]
           [:where_clause
            [:between_clause
             [:column_name "product" "prod-id"]
             [:long_literal "2000"]
             [:long_literal "3000"]]]]])
        {:type :select
         :fields [{:table "foo" :column "bar"}
                  [65 68 65 80 84 65 84 73 79 78 32 85 78 84 79 85 67
                   72 65 66 76 69 83]]
         :tables [{:name "foo"}]
         :where ['(:between {:table "product" :column "prod-id"}
                            2000 3000)]})

    ;; select foo.bar from product where :db/id = 17592186045445
    (is (= (prs/transform
            [:sql_data_statement
             [:select_statement
              [:select_list [:column_name "foo" "bar"]]
              [:from_clause [:table_ref [:table_name "product"]]]
              [:where_clause
               [:binary_comparison
                [:column_name "db" "id"]
                "="
                [:long_literal "17592186045445"]]]]])
           {:type :select,
            :fields [{:table "foo", :column "bar"}],
            :tables [{:name "product"}],
            :where ['(:db-id 17592186045445)]})
        "`:db/id = eid` translates to :db-id clause, not :binary_comparison")

    ;; update product set :product/category = :product.category/new, :product/tag = :foo-bar-baz, :product/actor = 'Quux the Great' where :product/prod-id = 1567
    (is (= (prs/transform
            [:sql_data_statement
             [:update_statement
              [:table_name "product"]
              [:set_clausen
               [:assignment_pair
                [:column_name "product" "category"]
                [:keyword_literal "product.category/new"]]
               [:assignment_pair
                [:column_name "product" "tag"]
                [:keyword_literal "foo-bar-baz"]]
               [:assignment_pair
                [:column_name "product" "actor"]
                [:string_literal "'Quux the Great'"]]]
              [:where_clause
               [:binary_comparison
                [:column_name "product" "prod-id"]
                "="
                [:long_literal "1567"]]]]])
           {:type :update,
            :table "product",
            :assign-pairs
            [[{:table "product", :column "category"} :product.category/new]
             [{:table "product", :column "tag"} :foo-bar-baz]
             [{:table "product", :column "actor"} "Quux the Great"]],
            :where ['(:= {:table "product", :column "prod-id"} 1567)]}))

    ;; delete from product where :product/prod-id between 2000 and 4000
    (is (= (prs/transform
            [:sql_data_statement
             [:delete_statement
              [:table_name "product"]
              [:where_clause
               [:between_clause
                [:column_name "product" "prod-id"]
                [:long_literal "2000"]
                [:long_literal "4000"]]]]])
           {:type :delete,
            :table "product",
            :where ['(:between {:table "product", :column "prod-id"}
                               2000 4000)]}))

    ;; insert into product (prod-id, actor, title, category) values (9999, 'Naomi Watts', 'The Ring', :product.category/horror)
    (is (= (prs/transform
            [:sql_data_statement
             [:insert_statement
              [:table_name "product"]
              [:insert_cols "prod-id" "actor" "title" "category"]
              [:insert_vals
               [:long_literal "9999"]
               [:string_literal "'Naomi Watts'"]
               [:string_literal "'The Ring'"]
               [:keyword_literal "product.category/horror"]]]])
           {:type :insert,
            :table "product",
            :cols ["prod-id" "actor" "title" "category"],
            :vals [9999 "Naomi Watts" "The Ring" :product.category/horror]}))

    ;; "select 42, 1234N, -12, -69N, 3.14159, 6.626E34, 1e-2, 2.7182M, 1.6182F from foobar"
    (is (= (prs/transform
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
               ]
              [:from_clause [:table_ref [:table_name "foobar"]]]]])
           {:type :select
            :fields [42
                     1234N
                     -12
                     -69N
                     3.14159
                     6.626E34
                     0.01
                     2.7182M
                     (float 1.6182)
                     ]
            :tables [{:name "foobar"}]}))
    ))
