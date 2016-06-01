(ns sql-datomic.parser-test
  (:require [clojure.test :refer :all]
            [sql-datomic.parser :as prs]
            [instaparse.core :as insta]))

(def good-parser? (complement insta/failure?))

(deftest select-tests
  (testing "SELECT statements"
    (is (good-parser? (prs/parser "SELECT name AS foo FROM a_table")))
    (is (good-parser? (prs/parser "SELECT name FROM a_table")))
    (is (good-parser? (prs/parser "SELECT name, age AS bleh FROM a_table")))
    (is (good-parser? (prs/parser "SELECT * FROM a_table")))
    (is (good-parser? (prs/parser "SELECT name FROM a_table, b_table")))
    (is (good-parser? (prs/parser "SELECT name FROM a_table WHERE name = 'foo'")))))
