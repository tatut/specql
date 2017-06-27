(ns specql.impl.catalog
  "Query the PostgreSQL catalog"
  (:require [clojure.java.jdbc :as jdbc]))


(def ^:private relkind-q "SELECT relkind FROM pg_catalog.pg_class WHERE relname=?")
(def ^:private columns-q (str "SELECT attr.attname AS name, attr.attnum AS number,"
                    "attr.attnotnull AS \"not-null?\","
                    "attr.atthasdef AS \"has-default?\","
                    "attr.atttypmod AS \"type-specific-data\","
                    "t.typname AS type, "
                    "t.typcategory AS category, "
                    "EXISTS(SELECT indkey FROM pg_catalog.pg_index i "
                    "        WHERE i.indrelid=attr.attrelid AND attr.attnum=ANY(i.indkey) AND i.indisprimary=TRUE) AS \"primary-key?\","
                    "EXISTS(SELECT e.enumtypid FROM pg_catalog.pg_enum e WHERE e.enumtypid = t.oid) AS \"enum?\""
                    "FROM pg_catalog.pg_attribute attr "
                    "JOIN pg_catalog.pg_class cls ON attr.attrelid = cls.oid "
                    "JOIN pg_catalog.pg_type t ON attr.atttypid = t.oid "
                    "WHERE cls.relname = ? AND attnum > 0"))
(def ^:private enum-values-q (str "SELECT e.enumlabel AS value"
                        "  FROM pg_type t"
                        "       JOIN pg_enum e ON t.oid = e.enumtypid"
                        "       JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace"
                        " WHERE t.typname=?"))

(def ^:private sproc-q
  (str "SELECT p.*, p.pronargs as argc, p.proargnames as argnames, "
       "ret.typname AS return_type, ret.typcategory AS return_category"
       " FROM pg_catalog.pg_proc p "
       " JOIN pg_catalog.pg_type ret ON p.prorettype = ret.oid"
       " WHERE p.proname = ?"))

(def ^:private sproc-args-types-q
  (str "SELECT t.typname AS type, t.typcategory AS category"
       " FROM pg_catalog.pg_type t"
       " WHERE t.oid = ANY(?)"))

(defn enum-values [db enum-type-name]
  (into #{}
        (map :value)
        (jdbc/query db [enum-values-q enum-type-name])))

(defn table-info [db name]
  (let [relkind (-> (jdbc/query db [relkind-q name])
                    first :relkind)]
    (if (empty? relkind)
      ;; Not a table or composite type, try an enum
      (let [values (enum-values db name)]
        (assert (not (empty? values))
                (str "Don't know what " name " is. Not a table, composite type or enum."))
        {:name name
         :type :enum
         :values values})

      (let [columns (jdbc/query db [columns-q name])]
        {:name name
         :type (case relkind
                 "c" :composite
                 "r" :table
                 "v" :view)
         :columns (into {}
                        (map (juxt :name identity))
                        columns)}))))
