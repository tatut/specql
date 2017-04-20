(ns specql.core
  (:require [clojure.spec :as s]
            [specql.data-types :as d]
            [specql.impl.def :as def]
            [specql.impl.jdbc]

            ;; Implementations of different specql operations.
            ;; Re-exported with docs from this namespace.
            [specql.impl.fetch :as fetch]
            [specql.impl.insert :as insert]
            [specql.impl.delete :as delete]
            [specql.impl.update :as update]))


(defmacro define-tables
  "Define specs and register query info for the given tables and user defined types.
  Tables are specified as [\"table-name\" :some.ns/table-name]. The table keys spec
  is registered for the table keyword and each column is registered with its name
  in the same namespace.

  For example if \"table-name\" table has columns \"id\" and \"name\" column specs
  are registered as keywords :some.ns/id and :some.ns/name respectively. The column
  specs are by determined by the SQL type.

  If a column is a composite that that has been previously registered, it is registered
  as the composite type."
  [db & tables]
  `(def/define-tables ~db ~@tables))

;; Re-export implementation functions here

(def
   ^{:doc "Fetch rows from the given tables.
  Db is a database connection (anything clojure.java.jdbc accepts).
  Table is a namespaced keyword indicating a previously defined database table.
  Columns is a set of namespaced keywords indicating the columns to fetch. A column can also be a
  vector of [keyword #{columns}] where the keyword indicates a joined table and the nested set the
  columns of the nested table to retrieve.
  Where clause is a map of column keyword to value or comparison operator. See specql.op namespace.

  Fetch returns a collection of maps.

  Example:
  (fetch db :employee/employees #{:employee/id :employee/name}
         {:employee/id (op/<= 2)})
  ;; => ({:employee/id 1 :employee/name \"Foo\"}
  ;;     {:employee/id 2 :employee/name \"Bar\"})"
     :arglists '([db table columns where])}
   fetch fetch/fetch)

;; FIXME: add docstrings

(def insert! insert/insert!)
(def upsert! insert/upsert!)
(def delete! delete/delete!)
(def update! update/update!)
