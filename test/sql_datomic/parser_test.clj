(ns sql-datomic.parser-test
  (:require [clojure.test :refer :all]
            [sql-datomic.parser :as prs]
            [instaparse.core :as insta]))

(def good-parser? (complement insta/failure?))

(defmacro parsable? [stmt]
  `(is (good-parser? (prs/parser ~stmt))))

(deftest select-tests
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
      set         city = 'Springfield'
                , state = 'VA'
                , zip = '22150'
      where       id = 123454321"))

  (testing "DELETE statements"
    (parsable?
     "delete from products where products.actor = 'homer simpson'")
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
