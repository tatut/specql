(ns specql.data-types
  (:require [clojure.spec :as s]))


(s/def ::int4 (s/int-in -2147483648 2147483647))
(s/def ::int8 (s/int-in -9223372036854775808 9223372036854775807))
(s/def ::varchar string?)

;; Postgres can handle upto 4713 BC, but do we need it?
(s/def ::date (s/inst-in #inst "0001-01-01T00:00:00.000-00:00"
                         #inst "9999-12-31T23:59:59.999-00:00"))
(s/def ::timestamp ::date)

(s/def ::numeric bigdec?)
(s/def ::text string?)
(s/def ::bool boolean?)
