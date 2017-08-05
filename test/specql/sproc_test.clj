(ns specql.sproc-test
  (:require  [clojure.test :as t :refer [deftest is testing]]
             [specql.embedded-postgres :refer [with-db datasource db]]
             [specql.core-test :refer [define-db]] ;; tables are defined in core test
             [clojure.java.jdbc :as jdbc]
             [specql.core :as specql :refer [insert! define-tables defsp]]
             [specql.transform :as xf]))

(t/use-fixtures :each with-db)


;; Define the user defined return type
(define-tables define-db
  ["issuetype-stats" :issue.stats/type-stats
   {:issue.stats/type (xf/transform (xf/to-keyword))}])

;; defsp = short form of define-stored-procedures macro
(defsp calculate-issuetype-stats define-db)

(defsp myrange define-db)

(defsp my-range-with-name define-db {::specql/name "myrange"})

;; Test baseline behaviour with raw SQL queries to the procedure

(defn- raw-stats [db]
  (jdbc/query db ["SELECT * FROM \"calculate-issuetype-stats\"('{\"open\"}'::status[], '')"]))

(defn- stats [db]
  (calculate-issuetype-stats db #{:issue.status/open} ""))

(defn- check-stats [db expected-raw]
  (let [r (raw-stats db)
        s (stats db)
        raw-vals (juxt (comp keyword :type) :percentage)
        spec-vals (juxt :issue.stats/type :issue.stats/percentage)]
    (is (= (set r) expected-raw))
    (is (= (map raw-vals r)
           (map spec-vals s)))))

(deftest calculate-issuetype-stats-calls
  (testing "Initially no issues, all types show zero percent"
    (check-stats db #{{:percentage 0.0M :type "feature"}
                      {:percentage 0.0M :type "bug"}}))

  (testing "After inserting a bug, that type shows 100 percent"
    (insert! db :issue/issue {:issue/status :issue.status/open
                              :issue/type :bug
                              :issue/title "the first issue"})
    (check-stats db #{{:percentage 100.00M :type "bug"}
                      {:percentage 0.00M :type "feature"}}))

  (testing "After inserting a feature, both are 50%"
    (insert! db :issue/issue {:issue/status :issue.status/open
                              :issue/type :feature
                              :issue/title "2nd issue"})
    (check-stats db #{{:percentage 50.00M :type "bug"}
                      {:percentage 50.00M :type "feature"}}))

  (testing "Add one more bug"
    (insert! db :issue/issue {:issue/status :issue.status/open
                              :issue/type :bug
                              :issue/title "3rd issue"})
    (check-stats db #{{:percentage 66.67M :type "bug"}
                      {:percentage 33.33M :type "feature"}}))

  (testing "Add issue in-progress"
    (insert! db :issue/issue {:issue/status :issue.status/in-progress
                              :issue/type :bug
                              :issue/title "doesn't affect stats"})
    (check-stats db #{{:percentage 66.67M :type "bug"}
                      {:percentage 33.33M :type "feature"}})))

(deftest myrange-calls
  (is (re-matches #".*successive integers.*"
                  (:doc (meta #'myrange) ""))
      "Docstring is valid")
  (is (= [1 2 3 4 5]
         (myrange db 1 6)
         (my-range-with-name db 1 6)))
  (is (= 1000
         (count (myrange db -500 500))
         (count (my-range-with-name db -500 500)))))

(def test-ns *ns*)

(deftest sp-with-wrong-name
  (is (thrown-with-msg?
       AssertionError #"No stored procedure found for name.*Hint:"
       (binding [*ns* test-ns]
         (eval '(defsp foobar define-db)))))
  (is (thrown-with-msg?
       AssertionError #".*No stored procedure found for name 'hep'."
       (binding [*ns* test-ns]
         (eval '(defsp foobar define-db {:specql.core/name "hep"}))))))
