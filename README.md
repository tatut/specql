[![CircleCI](https://circleci.com/gh/tatut/specql.svg?style=svg)](https://circleci.com/gh/tatut/specql)

# specql

An EXPERIMENTAL library for simple PostgreSQL queries with namespaced keys.

specql introspects your database at compile time and generates clojure.spec definitions for the rows
and all the columns.
You can then query for rows by giving a table, the keys to return and a map of keys to match against.


# usage

```clojure
(def db (make-my-db))

;; Define tables as [table-name :namespaced/table-keyword]
;; specql automatically creates specs for the columns of the table (in the same namespace as the
;; table keyword). For example if :employee/employees is the table and it has "id" and "name"
;; columns, specql will generate :employee/id and :employee/name specs with predicates
;; determined by the column data type
(define-tables define-db
  ["address" :address/address]
  ["employee" :employee/employees]
  ["department" :department/departments])

;; Querying

(fetch db
       ;; The table to query from
       :employee/employees
       ;; the columns to return
       #{:employee/id :employee/name :employee/title :employee/address}
       ;; the where clause
       {::employee/id 1})
;; => ({:employee/id 1
;;      :employee/name "Wile E. Coyote"
;;      :employee/title "Super genius"
;;      :employee/address {:address/street "Desert avenue 1"
;;                         :address/postal-code "31173"
;;                         :address/country "US"}})

```

# work in progress

This is very much still work in progress.

Features I intend to implement:
* update
* ~~insert~~
* JOIN navigation (add [:employee/department #{:department/name :department/id}] to JOIN the department table and fetch name and id keys as a nested map)
* ~~standard operators for where  ({:employee/name (like "%Smith%")})~~
* ~~unpacking composite types (user defined record types as column values should be nested maps)~~

# non-issues

This library is NOT intended to replace having to write SQL. The more complex SQL (like
reporting queries and complex joins) queries are better written as SQL. Use yesql/jeesql/hugsql
or the like.

This library intends to provide the most common case of CRUD queries and defer to SQL on the more difficult ones.