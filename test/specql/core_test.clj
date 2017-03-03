(ns specql.core-test
  (:require [specql.core :refer [define-tables fetch]]
            [clojure.test :as t :refer [deftest is]]
            [specql.embedded-postgres :refer [with-db datasource db]]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [clojure.spec :as s]))


(t/use-fixtures :each with-db)

(def define-db (datasource))

(define-tables define-db
  ["address" :address/address]
  ["employee" :employee/employees]
  ["department" :department/departments])

(deftest tables-have-been-created
  ;; If test data has been inserted, we know that all tables were create
  (is (= [{:companies 1}] (jdbc/query db ["SELECT COUNT(id) AS companies FROM company"]))))

(deftest simple-fetch
  (is (= (fetch db
                ;; Select from :employee/employees table
                :employee/employees

                ;; Return the following columns
                #{:employee/id :employee/name :employee/title}

                ;; Where clauses to match
                {:employee/id 1})

         (list #:employee{:id 1
                          :name "Wile E. Coyote"
                          :title "Super genious"}))))

(deftest query-unknown-table-or-column
  (is (thrown? AssertionError
               (eval '(specql.core/fetch db
                             :foo/bar
                             #{:foo/baz :foo/quux}
                             {}))))
  (is (thrown? AssertionError
               (eval '(specql.core/fetch db
                             :employee/employees
                             #{:employee/foo}
                             {})))))

(deftest query-with-invalid-parameter
  (let [x "foo"]
    (is (thrown-with-msg?
         AssertionError #"val: \"foo\" fails spec: :employee/id"
         (fetch db :employee/employees
                #{:employee/name}
                {:employee/id x})))))
