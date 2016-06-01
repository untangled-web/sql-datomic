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
    #_(good-stmt? "SELECT name FROM a_table WHERE name = 'foo'")))
