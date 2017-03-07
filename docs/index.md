# specql: easy PostgreSQL queries

specql makes querying PostgreSQL databases easy by introspecting tables at compile time and creating
specs for the rows and columns. Specql can then automatically do CRUD operations on the tables as
the table structure is known.

specql uses namespaced keys for all tables and columns and makes it easy to spot
errors at development time.

## Defining database tables

Specql works with specs and an internal table info registry which are built at compile time with the
`define-tables` macro. This requires that you have a valid database connection during
compile time (which you should have anyway, for testing).

> If your build process has no database available, you can use opentable/otj-pg-embedded to easily
> get an embedded database running.

The `define-tables` macro takes a database connection (anything clojure.java/jdbc supports) and
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
definition vector is a map of extra fields that are joined entitities. The `specql.rel` namespace
has helper functions for creating join definitions. Join specifications have three
parameters: column in this table, the table to join, the column in the joined table.

Note that the joined entity field should be different from the id field. For example if you name
a foreign reference column by the name name of the entity, you must call the joined
field something else (eg. "order" vs. "order-id"). It is more convenient to name foreign keys with a
"-id" prefix.