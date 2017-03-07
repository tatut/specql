# specql: easy PostgreSQL queries

specql makes querying PostgreSQL databases easy by introspecting tables at compile time and creating
specs for the rows and columns. Specql can then automatically do CRUD operations on the tables as
the table structure is known.

specql uses namespaced keys for all tables and columns and makes it easy to spot
errors at development time.

**TL;DR** To simple get a quick feel for what specql does, please look at the test suite:
* [database.sql](https://github.com/tatut/specql/blob/master/test/database.sql)
* [core_test.clj](https://github.com/tatut/specql/blob/master/test/specql/core_test.clj)


## Rationale

Creating a programmer friendly system to generate SQL is a hard problem. SQL is a big
language and either you end up mashing strings together or you try to encode the whole
language as data (or you make a poor SQL look-alike language which you then have to
generate by mashing strings together). Another solution is to punt on the problem
and just write SQL in resource files that can be loaded. That works well, but it
doesn't reduce the amount of boilerplate you have to write for different types of
queries.

Specql tries to solve the boilerplate problem for common cases and leave SQL where
it belongs (in the database). Specql introspects tables and provides a generic
fetch function which can query the introspected tables and return any projection
from them with any where filter. This removes the need to generate nearly identical
SQL queries for slightly different use cases.

Specql is opinionated in that it doesn't try to cover the full SQL language. For example aggregates
and any sort of reporting queries are simply not supported. With specql you write your
complex queries in the database and expose them as views which specql can then introspect
and work with. This has the added benefit that your data access queries are not coupled
to your application but instead live in the database where they belong.

## Defining database tables

Specql works with specs and an internal table info registry which are built at compile time with the
`define-tables` macro. This requires that you have a valid database connection during
compile time (which you should have anyway, for testing).

If your build process has no database available, you can use
[opentable/otj-pg-embedded](https://github.com/opentable/otj-pg-embedded) to easily
get an embedded database running.

The `define-tables` macro takes a database connection (anything
[clojure.java/jdbc](https://github.com/clojure/java.jdbc) supports) and
one or more vectors containing a table name, a keyword and optional join
configuration.

The table can be a regular database table, a composite user defined type, a view or an enum type.

```clojure
(def db (make-my-db))

(define-tables db
  ["customer" :customer/customers]
  ["order" :order/orders {:order/customer
                          (rel/has-one :order/customer-id
                                       :customer/customers
                                       :customer/id)}])
```

In the above example we define two tables "customer" and "order" with keywords
`:customer/customers` and `:order/orders` respectively. The table keywords can
be any namespaced keywords. Specql will automatically create specs for all columns
in the tables in the same namespace as the parent table keyword. So if customer
has columns "id", "name" and "address", the keywords `:customer/id`, `:customer/name`
and `:customer/address` will be registered as specs of the column types and added
to the tables information in the registry.

To avoid clashes, each table should have its own namespace. If you want to share a namespace for
example for a table and views about the same entity, make sure that the names are
consistent and have the same type in each table/view.

Specql supports JOINs and can navigate them while querying. The third element in the table
definition vector is a map of extra fields that are joined entities. The `specql.rel` namespace
has helper functions for creating join definitions. Join specifications have three
parameters: column in this table, the table to join, the column in the joined table.

Note that the joined entity keyword should be different from the id keyword. For example if you name
a foreign reference column by the name of the entity, you must call the joined
keyword something else (eg. "order" vs. "order-id"). It is more convenient to name foreign keys with
an "-id" prefix in the database so that the unprefixed name can be used for the joined entity.

## Querying data

The `fetch` function in the `specql.core` namespace is responsible for all queries.
It takes in a database connection and a description of what to query and returns a sequence
of maps.

### Simple example

```clojure
(fetch ;; the database connection to use
       db

       ;; the table to query from
       :order/orders

       ;; what columns to return
       #{:order/id :order/price}

       ;; where the following matches
       {:order/id 1})
;; => ({:order/id 1 :order/price 666M})
```

The following shows the basic form of a fetch call. The table is given with the same
keyword that was registered in `define-tables`. The columns to retrieve is a set
of column keywords in the table. The where clause is a map where keys are columns of
the table and values to compare against.

The keys in the returned maps will those that were specified in the columns set.

### Specifying search criteria

In the previous example, the where clause was generated with direct value comparisons.
Specql also supports common SQL operators in `specql.op` namespace:

* equality/ordinality: `=`, `not=`, `<`, `<=`, `>`, `>=`
* range: `between`
* text search: `like`
* set membership: `in`
* null checks: `null?` and `not-null?`
* combination: `or` and `and`
* negation: `not`

If a where map contains an operator instead of a value, the operator is called to generate
parameters and SQL. Keys in a where map are automatically ANDed together. A key value can
combine multiple operators with `and` or `or`. Combinations can also be used for whole
maps.

```clojure

;; Fetch orders in January
(fetch db :order/orders
       #{:order/id :order/price :order/item}
       {:order/date (op/between #inst "2017-01-01T00:00:00.000-00:00"
                                #inst "2017-01-31T23:59:59.999-00:00")})

;; Fetch recent or outstanding orders
(fetch db :order/orders
       #{:order/id :order/price :order/status :order/item}
       (op/or
        {:order/status (op/in #{"processing" "shipped"})}
	{:order/date (op/> (-> 14 days ago))}))
```

Given that where queries are made up of data and  specql validates the columns and
where criteria, it is feasible to let the client tell you what to fetch and how to
filter the result set without sacrificing security. You can use PostgreSQL row level
security to define what a user can see or simply AND a security where clause to the
query.

```clojure

(def orders-view-keys #{:order/date :order/status :order/price :order/id :order/item})

(defn user-orders
  "A where clause that restricts orders to the customer's own orders."
  [{id :user/id}]
  {:order/customer id})

(defn my-orders [db user search-criteria]
  (fetch db :order/orders orders-view-keys
         ;; AND together application defined criteria
	 ;; and client given filters
         (op/and (user-orders user)
	         search-criteria)))
```

In the above example the application has defined the criteria that is necessary for
security and can let the client side (for example to front end view) decide how to
filter. It can have a text search or date filter, or other restriction. The backend
code does not need to be changed to accommodate new front-end needs. The above example
can be made even more generic by letting the client decide the keys to fetch
(with a possible `clojure.set/difference` call on it to restrict it).

## Joining tables

NOTE: documentation coming soon

## Inserting new data

WIP: document insert! function
