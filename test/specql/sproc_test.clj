(ns specql.sproc-test
  (:require  [clojure.test :as t :refer [deftest is testing]]
             [specql.embedded-postgres :refer [with-db datasource db]]
             [specql.core-test :refer [define-db]] ;; tables are defined in core test
             [clojure.java.jdbc :as jdbc]
             [specql.core :as specql :refer [insert! define-tables defsp]]))

(t/use-fixtures :each with-db)

;; Test baseline behaviour with raw SQL queries to the procedure

(defn- raw-stats [db]
  (set (jdbc/query db ["SELECT * FROM \"calculate-issuetype-stats\"('{\"open\"}'::status[], '')"])))

(deftest raw-sproc-calls
  (testing "Initially no issues, all types show zero percent"
    (is (= #{{:percentage 0.0M :type "feature"}
             {:percentage 0.0M :type "bug"}}
           (raw-stats db))))

  (testing "After inserting a bug, that type shows 100 percent"
    (insert! db :issue/issue {:issue/status :issue.status/open
                              :issue/type :bug
                              :issue/title "the first issue"})
    (is (= #{{:percentage 100.00M :type "bug"}
             {:percentage 0.00M :type "feature"}}
           (raw-stats db))))

  (testing "After inserting a feature, both are 50%"
    (insert! db :issue/issue {:issue/status :issue.status/open
                              :issue/type :feature
                              :issue/title "2nd issue"})
    (is (= #{{:percentage 50.00M :type "bug"}
             {:percentage 50.00M :type "feature"}}
           (raw-stats db))))

  (testing "Add one more bug"
    (insert! db :issue/issue {:issue/status :issue.status/open
                              :issue/type :bug
                              :issue/title "3rd issue"})
    (is (= #{{:percentage 66.67M :type "bug"}
             {:percentage 33.33M :type "feature"}}
           (raw-stats db)))))

(println
 (pr-str
  (with-db #(jdbc/query db [@#'specql.impl.catalog/sproc-q "calculate-issuetype-stats"]))))

(comment
  (jdbc/with-db-connection [db define-db]
    (specql.impl.catalog/sproc-info db "calculate-issuetype-stats")))


;; Define the user defined return type
(define-tables define-db
  ["issuetype-stats" :issue.stats/type-stats])

;; defsp = short form of define-stored-procedures macro
(defsp calculate-issuetype-stats define-db)
