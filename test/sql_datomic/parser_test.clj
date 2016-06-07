(ns sql-datomic.parser-test
  (:require [clojure.test :refer :all]
            [sql-datomic.parser :as prs]
            [instaparse.core :as insta]))

(def good-parser? (complement insta/failure?))

(defmacro parsable? [stmt]
  `(is (good-parser? (prs/parser ~stmt))))

(deftest select-tests
  (testing "SELECT statements"
    (parsable? "SELECT name FROM a_table")
    (parsable? "SELECT name FROM a_table")
    (parsable? "SELECT name, age FROM a_table")
    (parsable? "SELECT * FROM a_table")
    (parsable? "SELECT name FROM a_table, b_table")
    (parsable?
     "SELECT foo
      FROM   a_table
     ")
    (parsable?
     "SELECT foo,
		bar,
             baz
      FROM   a_table
     ")
    (parsable?
     "
         SELECT foo FROM   a_table
     ")
    (parsable? "SELECT name FROM a_table WHERE name = 'foo'")
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
     "SELECT *
      FROM a_table
      WHERE a_table.foo IS NOT NULL")
    (parsable?
     "SELECT a_table.*, b_table.zebra_id
      FROM a_table
         , b_table
      WHERE a_table.id = b_table.a_id
        AND b_table.zebra_id IS NOT NULL
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
                 and    b_table.zebra_id is not null
      ")
    (parsable?
     "SELECT a_table.*, b_table.zebra_id
      FROM a_table, b_table
      WHERE  a_table.id = b_table.a_id
        AND  b_table.zebra_id > 9000")
    (parsable?
     "SELECT *
      FROM a_table
      WHERE a_table.created_on BETWEEN DATE '2007-02-01'
                                   AND DATE '2010-10-10'"))

  (testing "INSERT statements"
    (parsable?
     "insert into customers (
          firstname, lastname, address1, address2,
          city, state, zip, country
      ) values (
          'Foo', 'Bar', '123 Some Place', '',
          'Thousand Oaks', 'CA', '91362', 'USA'
      )
      "))

  (testing "UPDATE statements"
    (parsable?
     "update      customers
      set         city = 'Springfield'
                , state = 'VA'
                , zip = '22150'
      where       id = 123454321"))

  (testing "DELETE statements"
    (parsable?
     "delete from products where actor = 'homer simpson'")))
