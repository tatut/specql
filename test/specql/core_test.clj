(ns specql.core-test
  (:require [specql.core :refer [define-tables fetch insert! delete! update! upsert!
                                 columns tables]]
            [specql.op :as op]
            [specql.rel :as rel]
            [specql.transform :as xf]
            [clojure.test :as t :refer [deftest is testing]]
            [specql.embedded-postgres :refer [with-db datasource db]]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [clojure.spec.alpha :as s]
            [clojure.spec.test.alpha :as stest]))


(t/use-fixtures :each with-db)

(def define-db (datasource))

(define-tables define-db
  ["address" :address/address]
  ["employee" :employee/employees
   ;; Multiple option maps are automatically merged
   {;; Remap department with suffix, so that we
    ;; can use the unsuffixed for the JOIN
    "department" :employee/department-id}

   {:employee/department
    (rel/has-one :employee/department-id
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
                                                              :department-meeting/id)}]


  ["recipient" :recipient/recipient]
  ["mailinglist" :mailinglist/mailinglist]
  )

(defmacro asserted [msg-regex & body]
  `(is (~'thrown-with-msg?
        AssertionError ~msg-regex
        (do ~@body))))

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

(deftest query-with-unknown-where-column
  (is (thrown-with-msg?
       AssertionError #"no :address/country in table :employee/employees"
       (fetch db :employee/employees #{:employee/name}
              ;; :address/country is a valid column, but not in this table
              {:address/country "FI"})))

  (is (thrown-with-msg?
       AssertionError #"no :employee/name in composite type :address/address"
       (fetch db :employee/employees #{:employee/name}
              {:employee/address {:employee/name "Address has no name"}})))

  (is (thrown-with-msg?
       AssertionError #"no :address/country in table :employee/employees"
       (fetch db :employee/employees #{:employee/name}
              {:address/country (op/in #{"FI"})}))))

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
    (is (thrown-with-msg?
         AssertionError #"predicate: .*\(<= \(count"
         (insert! db :employee/employees
                  {:employee/name "too long addr"
                   :employee/employment-started (java.util.Date.)
                   :employee/address #:address{:street "this streetname is too long, max 20 chars"
                                               :postal-code "12345"
                                               :country "US"}})))

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
                     {:employee/employment-ended op/not-null?})))))

    (is (empty?
         (names {:employee/address {:address/country (op/in #{})}}))
        "Empty IN check matches nothing")))

(defn typetest [in]
  (let [inserted (insert! db :typetest/table in)
        queried (first
                 (fetch db :typetest/table
                        #{:typetest/int :typetest/numeric
                          :typetest/text :typetest/date
                          :typetest/bool :typetest/q :typetest/ts
                          :typetest/uuid :typetest/bytes}
                        {}))]
    (jdbc/execute! db "DELETE FROM typetest")
    queried))

(defn same-day? [& dates]
  (apply =
         (map (comp (juxt :date :month :year) bean) dates)))

(s/fdef typetest
        :args (s/cat :in :typetest/table-insert)
        :ret :typetest/table-insert
        :fn #(let [in (:in (:args %))
                   out (:ret %)]
               (and
                ;; other than dates and byte arrays, the maps are identical
                (= (dissoc in
                           :typetest/date :typetest/bytes)
                   (dissoc out
                           :typetest/date :typetest/bytes))
                ;; dates are on the same day
                (same-day? (:typetest/date in)
                           (:typetest/date out))

                ;; bytearrays have the same bytes
                (= (vec (:typetest/bytes in))
                   (vec (:typetest/bytes out))))))

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
                       :department #:department {:id 1 :name "R&D"}}
           (first
            (fetch db :employee/employees
                   #{:employee/name [:employee/department #{:department/id :department/name}]}
                   {:employee/id 1
                    :employee/department {:department/name (op/like "R%")}})))))

  (testing "join two levels: employee->department->company"
    (is (= #:employee {:name "Foo Barsky"
                       :department {:department/id 1 :department/company {:company/name "Acme Inc"}}}
           (first
            (fetch db :employee/employees
                   #{:employee/name [:employee/department
                                     #{:department/id [:department/company #{:company/name}]}]}
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
         AssertionError #"Unknown column :employee/no-such-field"
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
                                            :department-id 1
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

(deftest array-parsing
  (let [rcpt (fn [name street postal country]
               #:recipient{:name name
                           :address #:address{:street street
                                              :postal-code postal
                                              :country country}})]
    (testing "Values in arrays are parsed"
      (is (= #:mailinglist{:name "Fake News Quarterly"
                           :recipients
                           [(rcpt "Max Syöttöpaine" "Kujatie 1" "90100" "FI")
                            (rcpt "Erno Penttikoski" "Tiekuja 3" "90666" "FI")
                            (rcpt "Henna Lindberg" "Tiekuja 5" "4242" "FI")]})
          (first
           (fetch db :mailinglist/mailinglist
                  #{:mailinglist/name :mailinglist/recipients}
                  {}))))))

(deftest update-column-to-null
  (let [company #(first (fetch db :company/companies
                               #{:company/id :company/name :company/visiting-address}
                               {:company/id 1}))
        nil-count #(count (fetch db :company/companies #{:company/id}
                                 {:company/visiting-address op/null?}))]
    (is (zero? (nil-count)))
    (let [before (company)]
      (upsert! db :company/companies
               {:company/id 1
                :company/name (:company/name before)
                :company/visiting-address nil})
      (let [after (company)]
        (is (= after (dissoc before :company/visiting-address)))
        (is (= 1 (nil-count)))))))

(def test-ns *ns*)

(defn eval-ns [form]
  (binding [*ns* test-ns]
    (eval form)))

(deftest typeclash
  (asserted
   #":typeclash/start is already defined as \"timestamp\" and now trying to define it as \"time\""
   (eval-ns '(define-tables define-db
               ["typeclash1" :typeclash/one]
               ["typeclash2" :typeclash/two]))))

(deftest nameclash
  (asserted
   #"Table :nameclash/nameclash1 is also defined as a column"
   (eval-ns '(define-tables define-db
               ["nameclash1" :nameclash/nameclash1]
               ["nameclash2" :nameclash/nameclash2]))))

;; Tests for typos / errors in calling specql
(deftest errors
  (testing "Invalid columns set"
    (asserted #"Columns must be a non-empty set"
              (fetch db :employee/employees #{} {}))
    (asserted #"Columns must be a non-empty set"
              (fetch db :employee/employees [:this :is :not :a :set] {})))

  (testing "Invalid define-tables is caught"
    (asserted #"\"options\" fails spec"
              (eval-ns '(define-tables define-db
                          ["foo" :bar/sky
                           "options" :not-a-map])))

    (asserted #":tbl fails spec"
              (eval-ns '(define-tables define-db
                          [:tbl "bar"])))

    (asserted #"\"tablename\" fails spec"
              (eval-ns '(define-tables define-db
                          ;; not in vector
                          "tablename" :table/keyword))))

  (testing "Unknown JOIN throws error"
    (asserted
     #"Don't know how to fetch joined :deparment/no-such-join"
     (fetch db :department/departments
            #{:department/id
              [:deparment/no-such-join #{:no-such-thing/id}]}
            {})))

  (testing "Concise reporting of SQL connection errors"
    (asserted
     #"Unable to establish database connection to: "
     (eval-ns '(define-tables {:connection-uri "jdbc:postgresql://no-such-host:666/mydb"}
                 ["foo" :foo/bar])))))


;; Test custom field transformation

(define-tables define-db

  ;; The "status" enum is defined as its own type, the transformation will be
  ;; applied to any field whose type is the enum
  ["status" :issue.status/status (xf/transform (xf/to-keyword "issue.status"))]

  ["issue" :issue/issue
   ;; the "issuetype" enum is not defined as a type, but we can still transform
   ;; it when defining tables that use it
   {:issue/type (xf/transform (xf/to-keyword))}])

(deftest transformed-column-specs
  (testing "Keyword transformed spec validates properly"
    ;; All the valid namespaced keys are in fact valid
    (is (every? #(s/valid? :issue/status %) #{:issue.status/open
                                              :issue.status/in-progress
                                              :issue.status/resolved}))

    ;; An unnamespaced keyword is not valid
    (is (not (s/valid? :issue/status :open)))

    ;; Neither is a string in the enum
    (is (not (s/valid? :issue/status "in-progress")))))

(deftest insert-and-query-transformed
  (testing "Inserting with transformed values works"
    (insert! db :issue/issue {:issue/title "I have some issues"
                              :issue/status :issue.status/open}))

  (testing "Query returns the transformed value"
    (is (= (list {:issue/title "I have some issues"
                  :issue/status :issue.status/open})
           (fetch db :issue/issue #{:issue/title :issue/status} {}))))

  (testing "Updating a new value"
    (is (= 1 (update! db :issue/issue {:issue/status :issue.status/resolved}
                      {:issue/id 1}))))

  (testing "Where value with transformed works"
    (is (= (list {:issue/title "I have some issues"
                  :issue/status :issue.status/resolved})
           (fetch db :issue/issue #{:issue/title :issue/status}
                  {:issue/status :issue.status/resolved}))))

  (testing "Where operator works with transformed"
    (is (= (list {:issue/title "I have some issues"})
           (fetch db :issue/issue #{:issue/title}
                  {:issue/status (op/not= :issue.status/open)})))
    (is (= (list {:issue/title "I have some issues"})
           (fetch db :issue/issue #{:issue/title}
                  {:issue/status (op/in #{:issue.status/resolved})}))))

  (testing "Upsert transformed"
    (let [issue (upsert! db :issue/issue
                         #:issue {:title "foo" :status :issue.status/open
                                  :type :feature})]
      (is (= #:issue {:id 2 :title "foo" :status :issue.status/open
                      :type :feature}
             issue))

      (is (= (list issue)
             (fetch db :issue/issue #{:issue/id :issue/title :issue/status :issue/type}
                    {:issue/type :feature})))

      (let [issue-resolved (assoc issue :issue/status :issue.status/resolved)]
        (is (= issue-resolved (upsert! db :issue/issue issue-resolved)))

        (is (empty? (fetch db :issue/issue #{:issue/id} {:issue/status :issue.status/open})))

        (is (= (list issue-resolved)
               (fetch db :issue/issue #{:issue/id :issue/title :issue/status :issue/type}
                      {:issue/status :issue.status/resolved
                       :issue/title "foo"})))))))


(deftest query-registry
  (testing "Some of our defined tables are found in the registry"
    (is (every? (tables) [:employee/employees :issue/issue
                          :company/companies :department/departments])))

  (testing "Non-existant tables are not in the registry"
    (is (every? (complement (tables)) [:foo/bar :no-such/table])))

  (testing "Fields are returned"
    (is (= #{:issue/id :issue/title :issue/type :issue/description :issue/status}
           (columns :issue/issue)))))
