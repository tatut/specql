[![CircleCI](https://circleci.com/gh/tatut/specql.svg?style=svg)](https://circleci.com/gh/tatut/specql)
[![Clojars Project](https://img.shields.io/clojars/v/specql.svg)](https://clojars.org/specql)

# specql

A library for simple PostgreSQL queries with namespaced keys.

specql introspects your database at compile time and generates clojure.spec definitions for the rows
and all the columns.
You can then query for rows by giving a table, the keys to return and a map of keys to match against.

# intro talk

For a quick intro, see my talk about specql at ClojuTRE 2017
[Scrap your query boilerplate with specql](https://youtu.be/qEXNyZ5FJN4)

# usage

See the [docs](https://tatut.github.io/specql) for more examples.

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
       {:employee/id 1})
;; => ({:employee/id 1
;;      :employee/name "Wile E. Coyote"
;;      :employee/title "Super genius"
;;      :employee/address {:address/street "Desert avenue 1"
;;                         :address/postal-code "31173"
;;                         :address/country "US"}})

```

# Changes

### 20190301

* Fix composite type as primary key in `upsert!`

### 20181212

* Fix AOT compilation issue on Windows (no functionality changes)

### 20181211

* Support "int2" type in composite parsing

### 20180706

* Composite parsing speed improvement on large payloads: Avoid creating substrings when skipping quoted values.

### 20180312

* Changed to a date based versioning
* Added `specql.postgis` namespace that adds composite parsing support for geometry (requires postgis Java classes)

### 0.7.0-alpha18 (2018-03-03)

Birthday release! Specql is now 1 year old.

* Added support for MATERIALIZED VIEWs
* Added `refresh!` to refresh a materialized view
* Fixed `update!` with empty where clause
* Fixed SQL generation of an empty combined op on column level

### 0.7.0-alpha17 (2018-02-22)

* Added SQL exceptions provide generated SQL and parameters
* Fix storing `nil` value when in a `to-keyword` transformed composite

### 0.7.0-alpha16 (2018-02-05)

* Fixed yet another composite quoting issue
* Added generative test to create "interesting" composite data

### 0.7.0-alpha14 (2018-01-23)

* Fix: disambiguate `ORDER BY` column with table alias

### 0.7.0-alpha13 (2018-01-22)

* Quote type names in casts

### 0.7.0-alpha12 (2018-01-03)

* Add missing "bool" support in composite
* Fix composite parsing, skip quoted values when looking for a matching start/end pair (like "(" and ")").

### 0.7.0-alpha11

* Add spec for TIMESTAMP WITH TIME ZONE (timestamptz)

### 0.7.0-alpha10

* Fix reading of transformed type in a composite type

### 0.7.0-alpha9

* bigdec? was removed in Clojure 1.9.0-beta4, replaced with decimal? for compatibility

### 0.7.0-alpha8

* Fix composite array of primitive type parsing

### 0.7.0-alpha7

* Support "bpchar" (`CHAR(n)` type) in composite parsing
* Fix table quoting in DELETE
* Remove `clojure.future` requires as 1.9 is nearing release
* Add `:specql.core/transform-column-name` option to define-tables

### 0.7.0-alpha6

* Fix composite indexes when UDT attributes have been removed (with `ALTER TYPE`)

### 0.7.0-alpha5

* Apply `specql/transform` definitions to composite array elements when reading

### 0.7.0-alpha4

* Improved Reading of complex composites

### 0.7.0-alpha2 and -alpha3

* Fixed issue with `ROW(?,...)::typename` inserts with complex composites

### 0.7.0-alpha1

* Support stored procedures as functions
* Support order, limit and offset in query parameters

### 0.6

* Support custom column transformations (eg. db enums to keywords)
* Better enum support (operators add type casts to parameters)
* Operators now take a third argument: column info
* Support "point" type as vector [x y] in composites
* New functions: `columns` and `tables` for querying the runtime registry
* Improved error handling and better error messages


# work in progress

This is very much still work in progress.

Features I intend to implement:
* ~~fetch~~
* ~~update~~
* ~~insert~~
* ~~upsert~~
* ~~delete~~
* ~~JOIN navigation: has-one~~
* ~~JOIN navigation: has-many~~
* ~~standard operators for where  ({:employee/name (like "%Smith%")})~~
* ~~unpacking composite types (user defined record types as column values should be nested maps)~~

# non-issues

This library is NOT intended to replace having to write SQL. The more complex SQL (like
reporting queries and complex joins) queries are better written as SQL. Use yesql/jeesql/hugsql
or the like.

This library intends to provide the most common case of CRUD queries and defer to SQL on the more difficult ones.

Specql can use VIEWs in the database just like tables, so you can write your complex queries as
views and use them via specql.
