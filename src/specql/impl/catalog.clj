(ns specql.impl.catalog
  "Query the PostgreSQL catalog"
  (:require [clojure.java.jdbc :as jdbc]
            [specql.impl.registry :as registry]))


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
       "ret.typname AS return_type, ret.typcategory AS return_category, "
       "p.proretset AS return_set,"
       "d.description as comment "
       " FROM pg_catalog.pg_proc p "
       " JOIN pg_catalog.pg_type ret ON p.prorettype = ret.oid"
       " LEFT JOIN pg_catalog.pg_description d ON p.oid = d.objoid "
       " WHERE p.proname = ?"))

(def ^:private sproc-args-types-q
  (str "SELECT t.typname AS type, t.typcategory AS category"
       " FROM pg_catalog.pg_proc p "
       "      JOIN LATERAL unnest(p.proargtypes) WITH ORDINALITY AS a(oid,nr) ON TRUE "
       "      JOIN pg_catalog.pg_type t ON a.oid = t.oid "
       "WHERE p.proname=?"
       "ORDER BY a.nr ASC"))

(defn sproc-info [db sproc-name]
  (let [sproc (first (jdbc/query db [sproc-q sproc-name]))]
    {:name sproc-name
     :sproc sproc
     :comment (:comment sproc)
     :args (mapv (fn [name arg-info]
                   (as-> arg-info arg
                     (assoc arg :name name)
                     (if (= "A" (:category arg))
                       (assoc arg :element-type (registry/type-keyword-by-name (subs (:type arg) 1)))
                       arg)))
                 (seq (.getArray (:argnames sproc)))
                 (jdbc/query db [sproc-args-types-q sproc-name]))
     :returns {:type (:return_type sproc)
               :category (:return_category sproc)
               :single? (not (:return_set sproc))}}))

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
