(ns specql.core-test
  (:require [specql.core :refer [define-tables fetch insert!]]
            [clojure.test :as t :refer [deftest is testing]]
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
               (specql.core/fetch db
                                  :foo/bar
                                  #{:foo/baz :foo/quux}
                                  {})))
  (is (thrown? AssertionError
               (specql.core/fetch db
                                  :employee/employees
                                  #{:employee/foo}
                                  {}))))

(deftest query-with-invalid-parameter
  (let [x "foo"]
    (is (thrown-with-msg?
         AssertionError #"val: \"foo\" fails spec: :employee/id"
         (fetch db :employee/employees
                #{:employee/name}
                {:employee/id x})))))

(deftest composite-type-unpacking
  (is (= #:employee{:name "Wile E. Coyote"
                    :address #:address{:street "Desert avenue 1" :postal-code "31173" :country "US"}}
         (first
          (fetch db :employee/employees
                 #{:employee/name :employee/address}
                 {:employee/id 1})))))

(deftest inserting
  (testing "count before newly inserted rows"
    (is (= 2 (count (fetch db :employee/employees
                           #{:employee/id}
                           {})))))

  (testing "inserting two new employees"
    (is (= 3 (:employee/id (insert! db :employee/employees
                                    {:employee/name "Foo"}))))
    (is (= 4 (:employee/id (insert! db :employee/employees
                                    {:employee/name "Bar"})))))

  (testing "trying to insert invalid data"
    ;; Name field is NOT NULL, so insertion should fail
    (is (thrown-with-msg?
         AssertionError #"contains\? % :employee/name"
         (insert! db :employee/employees
                  {:employee/title "I have no name!"})))

    (is (thrown-with-msg?
         AssertionError #"val: 42 fails spec"
         (insert! db :employee/employees
                  {:employee/name "Foo"
                   :employee/title 42}))))

  (testing "querying for the new employees"
    (is (= #:employee{:id 3 :name "Foo"}
           (first (fetch db :employee/employees
                         #{:employee/id :employee/name}
                         {:employee/id 3})))))

  (testing "insert record with composite value"
    (let [addr #:address {:street "somestreet 123"
                          :postal-code "90123"
                          :country "US"}]
      (is (= 5 (:employee/id
                (insert! db :employee/employees
                         {:employee/name "Quux"
                          :employee/address addr}))))

      ;; Read the address back and verify it was properly saved
      (is (= addr (:employee/address
                   (first
                    (fetch db :employee/employees
                           #{:employee/address}
                           {:employee/id 5})))))

      ;; Check that validation failures in composite types are detected
      (is (thrown-with-msg?
           AssertionError #"val: 666 fails"
           (insert! db :employee/employees
                    {:employee/name "Frob"
                     :employee/address (assoc addr
                                              :address/postal-code 666)})))))

  (testing "count after insertions"
    (is (= 5 (count (fetch db :employee/employees
                           #{:employee/id}
                           {}))))))
