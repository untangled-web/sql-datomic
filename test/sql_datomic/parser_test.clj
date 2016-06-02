(ns sql-datomic.parser-test
  (:require [clojure.test :refer :all]
            [sql-datomic.parser :as prs]
            [instaparse.core :as insta]))

(def good-parser? (complement insta/failure?))

(defmacro parsable? [stmt]
  `(is (good-parser? (prs/parser ~stmt))))

(deftest select-tests
  (testing "SELECT statements"
    (parsable? "SELECT name AS foo FROM a_table")
    (parsable? "SELECT name FROM a_table")
    (parsable? "SELECT name, age AS bleh FROM a_table")
    (parsable? "SELECT * FROM a_table")
    (parsable? "SELECT name FROM a_table, b_table")
    (parsable?
     "SELECT foo AS \"fuu\"
      FROM   a_table
     ")
    (parsable?
     "SELECT foo AS \"fuu\",
		bar,
             baz AS hmm
      FROM   a_table
     ")
    (parsable?
     "
         SELECT foo FROM   a_table
     ")
    (parsable? "SELECT name FROM a_table WHERE name = 'foo'")
    (parsable?
     "SELECT a.*, b.zebra_id
      FROM a_table a
      JOIN b_table b ON a.id = b.a_id
      WHERE b.zebra_id > 9000")
    (parsable?
     "SELECT a.*, b.zebra_id
      FROM a_table a
      LEFT OUTER JOIN b_table b ON a.id = b.a_id
      ")
    (parsable?
     "SELECT a.*, b.zebra_id
      FROM a_table a
      LEFT OUTER JOIN b_table b ON a.id = b.a_id
      WHERE b.zebra_id > 9000
      ")
    (parsable?
     "SELECT *
      FROM a_table
      WHERE a_table.foo IS NOT NULL")
    (parsable?
     "SELECT a.*, b.zebra_id
      FROM a_table a
      LEFT OUTER JOIN b_table b ON a.id = b.a_id
      WHERE b.zebra_id IS NOT NULL
      ")
    (parsable?
     "select
                        a.*
                      , b.zebra_id
      from
                        a_table a
      left outer join
                        b_table b
                  on    a.id = b.a_id
      where
                        b.zebra_id is not null
      ")
    (parsable?
     "SELECT a.*, b.zebra_id
      FROM a_table a
      INNER JOIN b_table b ON a.id = b.a_id
      WHERE b.zebra_id > 9000")
    (parsable?
     "SELECT *
      FROM a_table a
      WHERE a.created_on BETWEEN DATE '2007-02-01'
                             AND DATE '2010-10-10'")))
