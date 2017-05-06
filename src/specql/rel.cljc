(ns specql.rel
  "Describe relationships between tables for JOIN support"
  (:require [clojure.spec.alpha :as s]))

(s/def ::rel (s/keys :req [::rel-type]
                     :opt [::this-table-column
                           ::other-table
                           ::other-table-column]))
(s/def ::rel-type #{::has-many ::has-one})
(s/def ::this-table-column keyword?)
(s/def ::other-table keyword?)
(s/def ::other-table-column keyword?)

(s/fdef has-many
        :args (s/cat :this-table-column ::this-table-column
                     :other-table ::other-table
                     :other-table-column ::other-table-column)
        :ret ::rel)

(defn has-many
  "Describe a link where another table links to this table by key"
  [this-table-column other-table other-table-column]
  {::type ::has-many
   ::this-table-column this-table-column
   ::other-table other-table
   ::other-table-column other-table-column})

(s/fdef has-one
        :args (s/cat :this-table-column ::this-table-column
                     :other-table ::other-table
                     :other-table-column ::other-table-column)
        :ret ::rel)

(defn has-one
  "Describe a link where this table links to another by key"
  [this-table-column other-table other-table-column]
  {::type ::has-one
   ::this-table-column this-table-column
   ::other-table other-table
   ::other-table-column other-table-column})
