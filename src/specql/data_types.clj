(ns specql.data-types
  (:require [clojure.spec :as s]))


(s/def ::integer int?)
(s/def ::text string?)
