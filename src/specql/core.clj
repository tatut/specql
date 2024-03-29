(ns specql.core
  (:require [clojure.spec.alpha :as s]
            [specql.data-types :as d]
            [specql.impl.def :as def]
            [specql.impl.jdbc]

            ;; Implementations of different specql operations.
            ;; Re-exported with docs from this namespace.
            [specql.impl.fetch :as fetch]
            [specql.impl.insert :as insert]
            [specql.impl.delete :as delete]
            [specql.impl.update :as update]
            [specql.impl.registry :as registry]
            [specql.impl.sproc :as sproc]
            [specql.impl.catalog :as catalog]))


(defmacro define-tables
  "Define specs and register query info for the given tables and user defined types.
  Tables are specified as [\"table-name\" :some.ns/table-name]. The table keys spec
  is registered for the table keyword and each column is registered with its name
  in the same namespace.

  For example if \"table-name\" table has columns \"id\" and \"name\" column specs
  are registered as keywords :some.ns/id and :some.ns/name respectively. The column
  specs are by determined by the SQL type.

  If a column is a composite that that has been previously registered, it is registered
  as the composite type.


  An options map can be given instead of the database connection.
  The options map must contain the key `:specql.core/db` which is the database
  connection to use or `:specql.core/schema-file` (string) that points to a classpath
  resource containing prefetched database schema information.
  The schema-file can be generated by calling `(specql.core/create-schema-file! db)`.

  Other supported options:
  :specql.core/transform-column-name   a function to transform a column name to a namespaced keyword.
                                       Takes 2 arguments: the namespace and the column name.
                                       Must return a qualified keyword.
  "
  [db & tables]
  `(def/define-tables ~db ~@tables))

;; Re-export implementation functions here

(defn fetch
  "Fetch rows from the given tables.
  Db is a database connection (anything clojure.java.jdbc accepts).
  Table is a namespaced keyword indicating a previously defined database table.
  Columns is a set of namespaced keywords indicating the columns to fetch. A column can also be a
  vector of [keyword #{columns}] where the keyword indicates a joined table and the nested set the
  columns of the nested table to retrieve.
  Where clause is a map of column keyword to value or comparison operator. See specql.op namespace.

  An optional options map may be provided to alter the query behaviour.
  Supported options are:
    :specql.core/order-by   a column keyword in the queried table to order the results by
    :specql.core/order-direction   sort direction, either :ascending (default) or :descending
    :specql.core/limit      limit result set size to this many rows
    :specql.core/offset     skip first N result set rows

  Note about limit/offset use: limit and offset work at the result set level.
  If the query has a has-many join, it will have possibly multiple rows for
  each entry in the base table. SQL does not guarantee result ordering if an
  order by is not used, so limit/offset will be inconsistent without ordering.

  Fetch returns a collection of maps.

  Example:
  (fetch db :employee/employees #{:employee/id :employee/name}
         {:employee/id (op/<= 2)})
  ;; => ({:employee/id 1 :employee/name \"Foo\"}
  ;;     {:employee/id 2 :employee/name \"Bar\"})"
  ([db table columns where]
   (fetch db table columns where {}))
  ([db table columns where options]
   (fetch/fetch db table columns where options)))

;; FIXME: improve docstrings

(defn insert!
  "Insert a record to the given table. Returns the inserted record with the
  (possibly generated) primary keys added."
  [db table-kw record]
  (insert/insert! db table-kw record))

(defn upsert!
  "Atomically UPDATE or INSERT a record"
  [db table & keyset-record-where]
  (apply insert/upsert! db table keyset-record-where))

(defn delete!
  "Delete rows from table that match the given search criteria.
  Returns the number of rows deleted."
  [db table where]
  (delete/delete! db table where))

(defn update!
  "Update matching records. Returns number of records updated."
  [db table record where]
  (update/update! db table record where))


(defn refresh!
  "Refresh a materialized view."
  [db materialized-view-table]
  (update/refresh! db materialized-view-table))


;; Functions to query the runtime information

(defn columns
  "Return set of all column names in the given table.
  Only the base columns of the table are returned.
  If the given table has not been defined, returns nil."
  [table-kw]
  (registry/columns table-kw))

(defn tables
  "Returns a set containing all defined table keywords"
  []
  (registry/tables))


;; Define stored procedure

(defmacro define-stored-procedures [db & procedures]
  `(sproc/define-stored-procedures
     ~db
     ~@procedures))

(defmacro defsp [fn-name db & options-map]
  `(sproc/define-stored-procedures
     ~db
     [~fn-name ~(first options-map)]))

(defn create-schema-file! [& db-and-optional-file]
  (apply catalog/create-schema-file! db-and-optional-file))
