(ns specql.impl.composite-test
  (:require [clojure.test :refer :all]
            [specql.impl.composite :as composite]))

(is (= (composite/parse-value "int4" "NULL") nil))
(is (= (composite/parse-value "int4" "3") 3))