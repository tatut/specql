(ns specql.data-types
  "Defines specs for PostgreSQL database defined types."
  (:require [clojure.spec.alpha :as s]))

(s/def ::int2 (s/int-in -32768 32767))
(s/def ::int4 (s/int-in -2147483648 2147483647))
(s/def ::uint4 (s/int-in 0 4294967295))
(s/def ::int8 (s/int-in -9223372036854775808 9223372036854775807))
(s/def ::float8 double?)
(s/def ::varchar string?)

;; Postgres can handle upto 4713 BC, but do we need it?
(s/def ::date #?(:clj (s/inst-in #inst "0001-01-01T00:00:00.000-00:00"
                                 #inst "9999-12-31T23:59:59.999-00:00")
                 :cljs any?))
(s/def ::timestamp ::date)
(s/def ::timestamptz ::date)
(s/def ::time #?(:clj #(instance? java.time.LocalTime %)

                 ;; FIXME: what is time in cljs?
                 :cljs any?))

(s/def ::numeric #?(:clj decimal?
                    :cljs number?))
(s/def ::text string?)
(s/def ::bpchar string?)
(s/def ::bool boolean?)
(s/def ::oid ::uint4)

(s/def ::uuid uuid?)
(s/def ::bytea #?(:clj bytes?
                  ;; FIXME: what's bytes in clojurescript?
                  :cljs any?))

;; Geometry types
(s/def ::point (s/coll-of number?))

;; PENDING:
;; We don't currently have good specs for the following types.
;; Users probably want to transform to some clojure data representation
(s/def ::geometry any?)
(s/def ::jsonb any?) ;; in JDBC a PGobject containing string representation

;; FIXME: support more postgres types
#_(remove #(or (str/starts-with? % "pg_") (str/starts-with? % "_"))
        (map :typname (jdbc/query db [ "SELECT distinct(typname) from pg_type"])))


(def db-types
  #{::int2 ::int4 ::int8 ::float8 ::numeric
    ::varchar ::text ::bpchar
    ::date ::timestamp ::time
    ::bool
    ::uuid
    ::bytea
    ::point ::geometry
    ::jsonb})

(defn db-type? [type]
  (contains? db-types type))
