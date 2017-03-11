(ns specql.core-test
  (:require [specql.core :refer [define-tables fetch insert! delete! update! upsert!]]
            [specql.op :as op]
            [specql.rel :as rel]
            [clojure.test :as t :refer [deftest is testing]]
            [specql.embedded-postgres :refer [with-db datasource db]]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [clojure.spec :as s]
            [clojure.spec.test :as stest]))


(t/use-fixtures :each with-db)

(def define-db (datasource))

(define-tables define-db
  ["address" :address/address]
  ["employee" :employee/employees {:employee/dep (rel/has-one :employee/department
                                                              :department/departments
                                                              :department/id)}]
  ["company" :company/companies]
  ["department" :department/departments {:department/employees
                                         (rel/has-many :department/id
                                                       :employee/employees
                                                       :employee/department)
                                         :department/company
                                         (rel/has-one :department/company-id
                                                      :company/companies
                                                      :company/id)}]
  ["quark" :enum/quark] ;; an enum type
  ["typetest" :typetest/table]

  ;; a view is also a table
  ["department-employees" :dep-employees/view]

  ["department-meeting" :department-meeting/meetings
   {:department-meeting/department1 (rel/has-one :department-meeting/department1-id
                                                 :department/departments
                                                 :department/id)
    :department-meeting/department2 (rel/has-one :department-meeting/department2-id
                                                 :department/departments
                                                 :department/id)
    :department-meeting/notes (rel/has-many :department-meeting/id
                                            :department-meeting-notes/notes
                                            :department-meeting-notes/department-meeting-id)}]
  ["department-meeting-notes" :department-meeting-notes/notes
   {:department-meeting-notes/department-meeting (rel/has-one :department-meeting-notes/department-meeting-id
                                                              :department-meeting/meetings
                                                              :department-meeting/id)}])

(deftest tables-have-been-created
  ;; If test data has been inserted, we know that all tables were create
  (is (= [{:companies 2}] (jdbc/query db ["SELECT COUNT(id) AS companies FROM company"]))))

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
    (is (= 3 (count (fetch db :employee/employees
                           #{:employee/id}
                           {})))))

  (testing "inserting two new employees"
    (is (= 4 (:employee/id (insert! db :employee/employees
                                    {:employee/name "Foo"
                                     :employee/employment-started (java.util.Date.)}))))
    (is (= 5 (:employee/id (insert! db :employee/employees
                                    {:employee/name "Bar"
                                     :employee/employment-started (java.util.Date.)})))))

  (testing "trying to insert invalid data"
    ;; Name field is NOT NULL, so insertion should fail
    (is (thrown-with-msg?
         AssertionError #"contains\? % :employee/name"
         (insert! db :employee/employees
                  {:employee/title "I have no name!"
                   :employee/employment-started (java.util.Date.)})))

    (is (thrown-with-msg?
         AssertionError #"val: 42 fails spec"
         (insert! db :employee/employees
                  {:employee/name "Foo"
                   :employee/employment-started (java.util.Date.)
                   :employee/title 42}))))

  (testing "querying for the new employees"
    (is (= #:employee{:id 4 :name "Foo"}
           (first (fetch db :employee/employees
                         #{:employee/id :employee/name}
                         {:employee/id 4})))))

  (testing "insert record with composite value"
    (let [addr #:address {:street "somestreet 123"
                          :postal-code "90123"
                          :country "US"}]
      (is (= 6 (:employee/id
                (insert! db :employee/employees
                         {:employee/name "Quux"
                          :employee/employment-started (java.util.Date.)
                          :employee/address addr}))))

      ;; Read the address back and verify it was properly saved
      (is (= addr (:employee/address
                   (first
                    (fetch db :employee/employees
                           #{:employee/address}
                           {:employee/id 6})))))

      ;; Check that validation failures in composite types are detected
      (is (thrown-with-msg?
           AssertionError #"val: 666 fails"
           (insert! db :employee/employees
                    {:employee/name "Frob"
                     :employee/address (assoc addr
                                              :address/postal-code 666)})))))

  (testing "count after insertions"
    (is (= 6 (count (fetch db :employee/employees
                           #{:employee/id}
                           {}))))))

(deftest query-with-composite-value
  (testing "query companies by visiting address country"
    (is (= 2 (count
              (fetch db :company/companies
                     #{:company/name :company/visiting-address}
                     {:company/visiting-address {:address/country "FI"}}))))))

(deftest query-operators
  ;; There are no companies whose visiting address is not in Finland
  (is (empty?
       (fetch db :company/companies
              #{:company/id}
              {:company/visiting-address {:address/country (op/not= "FI")}})))

  (is (= 1
         (count (fetch db :employee/employees
                       #{:employee/id}
                       {:employee/employment-started (op/< #inst "1997-08-04T02:14:30.798-04:00")}))))

  (let [names #(into #{}
                     (map :employee/name
                          (fetch db :employee/employees
                                 #{:employee/name}
                                 %)))]
    (is (= #{"Wile E. Coyote"}
           (names {:employee/name (op/like "%yo%")})))

    (is (= #{"Max Syöttöpaine" "Foo Barsky"}
           (names {:employee/name (op/not (op/like "%yo%"))})))

    (is (= #{"Foo Barsky" "Wile E. Coyote"}
           (names {:employee/name (op/or (op/like "%yo%")
                                         (op/like "%sky"))})))

    (is (= #{"Max Syöttöpaine"}
           (names {:employee/name (op/and (op/like "%a%")
                                          (op/like "%x%"))})))

    (is (= #{"Foo Barsky"}
           (names {:employee/employment-ended op/not-null?})))

    (testing "or works on whole maps"
      (is (= #{"Wile E. Coyote" "Foo Barsky"}
             (names (op/or
                     {:employee/address {:address/country (op/in #{"US"})}}
                     {:employee/employment-ended op/not-null?})))))))

(defn typetest [in]
  (let [inserted (insert! db :typetest/table in)
        queried (first
                 (fetch db :typetest/table
                        #{:typetest/int :typetest/numeric
                          :typetest/text :typetest/date
                          :typetest/bool :typetest/q :typetest/ts}
                        {}))]
    (jdbc/execute! db "DELETE FROM typetest")
    queried))

(defn same-day? [& dates]
  (apply =
         (map (comp (juxt :date :month :year) bean) dates)))

(s/fdef typetest
        :args (s/cat :in :typetest/table-insert)
        :ret :typetest/table-insert
        :fn #(and
              ;; other than dates, the maps are identical
              (= (dissoc (:in (:args %))
                         :typetest/date)
                 (dissoc (:ret %)
                         :typetest/date))
              ;; dates are on the same day
              (same-day? (:typetest/date (:in (:args %)))
                         (:typetest/date (:ret %)))))

(deftest typetest-generate-and-query
  (is (= {:total 1 :check-passed 1}
         (stest/summarize-results (stest/check `typetest)))))

(deftest view-query
  (is (= #{#:dep-employees{:id 1 :name "R&D" :employee-count 3}
           #:dep-employees{:id 2 :name "Marketing" :employee-count 0}}
         (into #{}
               (fetch db :dep-employees/view
                      #{:dep-employees/id
                        :dep-employees/name
                        :dep-employees/employee-count}
                      {}))))

  ;; Can't insert to a view
  (is (thrown? java.sql.SQLException
               (insert! db :dep-employees/view
                        {:dep-employees/id 1
                         :dep-employees/name "R&D"
                         :dep-employees/employee-count 666}))))

(defn- format-meeting [{d1 :department-meeting/department1
                        d2 :department-meeting/department2
                        subject :department-meeting/subject}]
  (format "A meeting on '%s' between the %s department and the %s department"
          subject (:department/name d1) (:department/name d2)))

(deftest join-has-one
  (testing "simple join"
    (is (= #:employee {:name "Wile E. Coyote"
                       :dep #:department {:id 1 :name "R&D"}}
           (first
            (fetch db :employee/employees
                   #{:employee/name [:employee/dep #{:department/id :department/name}]}
                   {:employee/id 1
                    :employee/dep {:department/name (op/like "R%")}})))))

  (testing "join two levels: employee->department->company"
    (is (= #:employee {:name "Foo Barsky"
                       :dep {:department/id 1 :department/company {:company/name "Acme Inc"}}}
           (first
            (fetch db :employee/employees
                   #{:employee/name [:employee/dep #{:department/id
                                                     [:department/company #{:company/name}]}]}
                   {:employee/id 3})))))

  (testing "join the same table twice"
    (is (= "A meeting on 'ad campaigns for new widgets' between the R&D department and the Marketing department"
           (format-meeting
            (first
             (fetch db :department-meeting/meetings
                    #{:department-meeting/subject
                      [:department-meeting/department1 #{:department/name}]
                      [:department-meeting/department2 #{:department/name}]}
                    {})))))))

(deftest join-has-many
  (testing "fetch meetings with the notes"
    (is (= (list #:department-meeting{:id 1 :subject "ad campaigns for new widgets"
                                      :notes
                                      [#:department-meeting-notes{:note "Rolf suggested a new campaign called: widgets4all", :time #inst "2017-03-07T09:01:00.000000000-00:00"}
                                       #:department-meeting-notes{:note "Max seconded the idea, but asked for cost estimates", :time #inst "2017-03-07T09:02:00.000000000-00:00"}
                                       #:department-meeting-notes{:note "After lengthy dicussion, it was decided that RFPs would be sent to the usual ad agencies", :time #inst "2017-03-07T09:45:00.000000000-00:00"}]})
           (fetch db :department-meeting/meetings
                  #{:department-meeting/id
                    :department-meeting/subject
                    [:department-meeting/notes
                     #{:department-meeting-notes/time
                       :department-meeting-notes/note}]}
                  {})))))

(deftest delete
  (let [fetch-emp1 #(first (fetch db :employee/employees
                                  #{:employee/name} {:employee/id 1}))]
    (testing "deletion works"
      ;; Wile exists
      (is (= {:employee/name "Wile E. Coyote"} (fetch-emp1)))

      ;; Delete any employee with id less than 2 (should return 1 deleted rows)
      (is (= 1 (delete! db :employee/employees {:employee/id (op/< 2)})))

      ;; Wile does not exist anymore
      (is (empty? (fetch-emp1)))))

  (testing "delete with an empty clause will throw"
    (is (thrown-with-msg?
         AssertionError #"empty where clause"
         (delete! db :employee/employees
                  {}))))

  (testing "delete from an unknown table"
    (is (thrown-with-msg?
         AssertionError #"Unknown table"
         (delete! db :foo/bar {:foo/id 1})))))

(deftest updating
  (testing "simple update"
    (is (= 1 (update! db :employee/employees
                      {:employee/name "Quux Barsky"}
                      {:employee/id 3})))

    (is (= "Quux Barsky"
           (:employee/name
            (first
             (fetch db :employee/employees
                    #{:employee/name}
                    {:employee/id 3}))))))

  (testing "update unknown columns"
    (is (thrown-with-msg?
         AssertionError #"Unknown columns"
         (update! db :employee/employees
                  {:employee/name "foo"
                   :employee/no-such-field "bar"}
                  {:employee/id 1})))))

(deftest upsert
  (testing "update existing"

    (let [emp3 #(first (fetch db :employee/employees
                              #{:employee/name :employee/employment-started}
                              {:employee/id 3}))
          start (java.util.Date.)]

      (is (= {:employee/name "Foo Barsky"
              :employee/employment-started #inst "2010-07-07T00:00:00.000-00:00"}
             (emp3)))

      ;; Upsert changes to employee 3
      (is (= {:employee/name "Bar Foosky"
              :employee/employment-started start
              :employee/id 3}
             (upsert! db :employee/employees
                      {:employee/id 3
                       :employee/name "Bar Foosky"
                       :employee/employment-started start})))

      ;; Fetching it again returns the new values
      (let [fetched (emp3)]
        (is (same-day? (:employee/employment-started fetched) start))
        (is (= "Bar Foosky" (:employee/name fetched))))

      ;; Upsert with a where clause that does not match

      (is (nil? (upsert! db :employee/employees
                      {:employee/id 3
                       :employee/name "NOT CHANGED"
                       :employee/employment-started start}
                      {:employee/employment-ended op/null?})))

      ;; Verify that name has not been changed
      (is (= "Bar Foosky" (:employee/name
                           (first
                            (fetch db :employee/employees #{:employee/name}
                                   {:employee/id 3})))))))


  (testing "upsert new rows"
    (let [existing-ids (into #{}
                             (map :employee/id)
                             (fetch db :employee/employees #{:employee/id} {}))
          new-employee (upsert! db :employee/employees
                                #:employee {:name "Rolf Teflon"
                                            :department 1
                                            :employment-started (java.util.Date.)})]
      (is (contains? new-employee :employee/id))
      (is (not (existing-ids (:employee/id new-employee)))))))

(deftest upsert-with-columns
  (testing "upsert with index columns"
    ;; Change a meeting note via upsert

    (let [notes #(fetch db :department-meeting-notes/notes
                        #{:department-meeting-notes/department-meeting-id
                          :department-meeting-notes/time
                          :department-meeting-notes/note}
                        {})]
      (is (= 3 (count (notes))))
      (let [changed-note
            #:department-meeting-notes {:department-meeting-id 1
                                        :time #inst "2017-03-07T09:01:00.000-00:00"
                                        :note "NOTE CHANGED"}]

        (is (thrown-with-msg?
             AssertionError #"No conflict target"
             ;; upsert fails, table has no primary key and we havent
             ;; specified a conflict target
             (upsert! db :department-meeting-notes/notes changed-note)))

        (is (= changed-note
               (upsert! db
                        :department-meeting-notes/notes
                        #{:department-meeting-notes/department-meeting-id
                          :department-meeting-notes/time}
                        changed-note)))

        ;; Note count is still the same
        (is (= 3 (count (notes))))

        ;; The changed text is there
        (is (some #(= "NOTE CHANGED" (:department-meeting-notes/note %))
                  (notes)))

        ;; Test non-matching where clause
        (is (nil?
             (upsert! db
                      :department-meeting-notes/notes
                      #{:department-meeting-notes/department-meeting-id
                        :department-meeting-notes/time}
                      changed-note
                      {:department-meeting-notes/note "THIS WONT MATCH ANYTHING"})))
        (is (= 3 (count (notes))))


        ;; Change time so that a new row is inserted
        (upsert! db :department-meeting-notes/notes
                 #{:department-meeting-notes/department-meeting-id
                   :department-meeting-notes/time}
                 #:department-meeting-notes {:department-meeting-id 1
                                             :time (java.util.Date.)
                                             :note "NOTE ADDED"})

        (is (= 4 (count (notes))))
        (is (some #(= "NOTE ADDED" (:department-meeting-notes/note %))
                  (notes)))))))
