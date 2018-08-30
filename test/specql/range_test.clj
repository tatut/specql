(ns specql.range-test
  (:require [clojure.test :as t :refer [deftest is testing]]
            [specql.core :refer [define-tables fetch insert! update!] :as specql]
            [specql.embedded-postgres :refer [with-db datasource db]]
            [specql.core-test :refer [define-db]]
            [specql.data-types :as dt]))


(t/use-fixtures :each with-db)

(define-tables define-db
  ["part_price_range" :range-test/part-price-range-type]
  ["price_range" :range-test/price-range])

(deftest query-test-data
  (let [result (specql/fetch db :range-test/price-range
                             #{:range-test/name :range-test/total-range :range-test/part-price-ranges}
                             {:range-test/name "medium systems vehicle"})]
    (is (= 1 (count result)))

    (is (= (first result)
           #:range-test {:name "medium systems vehicle"
                         :total-range "foo"
                         :part-price-ranges [#:range-test {:name "engine"
                                                           :part-price-range (dt/->Range 1 10001 true false)}
                                             #:range-test {:name "cargo bay"
                                                           :part-price-range (dt/->Range 123 100000 true false)}]}))))
