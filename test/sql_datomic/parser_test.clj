(ns sql-datomic.parser-test
  (:require [clojure.test :refer :all]
            [sql-datomic.parser :as prs]
            [instaparse.core :as insta]))

(def good-parser? (complement insta/failure?))

(defmacro good-stmt? [stmt]
  `(is (good-parser? (prs/parser ~stmt))))

(deftest select-tests
  (testing "SELECT statements"
    (good-stmt? "SELECT name AS foo FROM a_table")
    (good-stmt? "SELECT name FROM a_table")
    (good-stmt? "SELECT name, age AS bleh FROM a_table")
    (good-stmt? "SELECT * FROM a_table")
    (good-stmt? "SELECT name FROM a_table, b_table")
    (good-stmt? "SELECT name FROM a_table WHERE name = 'foo'")))
