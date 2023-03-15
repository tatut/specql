(ns specql.impl.catalog
  "Query the PostgreSQL catalog"
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.java.io :as io]
            [specql.impl.registry :as registry]
            [specql.impl.util :as util]))


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

(defn- with-element-type [type]
  (if (= "A" (:category type))
    (assoc type :element-type (subs (:type type) 1))
    type))

(defprotocol SchemaInfo
  (sproc-info* [this sproc-name])
  (enum-values* [this enum-type-name])
  (table-info* [this name]))

(defrecord DbSchemaInfo [db]
  SchemaInfo
  (sproc-info* [_ sproc-name]
    (let [sproc (first (jdbc/query db [sproc-q sproc-name]))]
      (when sproc
        {:name sproc-name
         :comment (:comment sproc)
         :args (mapv (fn [name arg-info]
                       (as-> arg-info arg
                         (assoc arg :name name)
                         (with-element-type arg)))
                     (seq (.getArray (:argnames sproc)))
                     (jdbc/query db [sproc-args-types-q sproc-name]))
         :returns (with-element-type
                    {:type (:return_type sproc)
                     :category (:return_category sproc)
                     :single? (not (:return_set sproc))})})))

  (enum-values* [_ enum-type-name]
    (into #{}
          (map :value)
          (jdbc/query db [enum-values-q enum-type-name])))

  (table-info* [this name]
    (let [relkind (-> (jdbc/query db [relkind-q name])
                      first :relkind)]
      (if (empty? relkind)
        ;; Not a table or composite type, try an enum
        (let [values (enum-values* this name)]
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
                   "v" :view
                   "m" :materialized-view)
           :columns (into {}
                          (map (juxt :name identity))
                          columns)}))))
  java.lang.AutoCloseable
  (close [_]
    (.close (:connection db))))

(defrecord CachedSchemaInfo [info]
  SchemaInfo
  (sproc-info* [_ sproc-name]
    (get-in info [:sprocs sproc-name]))

  (enum-values* [_ enum-type-name]
    (get-in info [:enums enum-type-name]))

  (table-info* [this name]
    (if-let [table (get-in info [:tables name])]
      table
      (let [values (enum-values* this name)]
        (assert (not (empty? values))
                (str "Don't know what " name " is. Not a table, composite type or enum."))
        {:name name
         :type :enum
         :values values})))

  java.lang.AutoCloseable
  (close [this]
    ;; NOP
    ))

(defn ->schema-info [x]
  (if (and (map? x) (contains? x :specql.core/schema-file))
    (->schema-info (:specql.core/schema-file x))
    (if (string? x)
      (CachedSchemaInfo. (-> x io/resource slurp read-string))
      (DbSchemaInfo. {:connection (util/connect x)}))))

(defn sproc-info [this sproc-name]
  (sproc-info* this sproc-name))

(defn enum-values [this enum-type-name]
  (enum-values* this enum-type-name))

(defn table-info [this name]
  (table-info* this name))

(defn create-schema-file!
  ([db] (create-schema-file! db "resources/specql-schema.edn"))
  ([db schema-file]
   (jdbc/with-db-connection [con db]
     (let [s (->schema-info db)
           enum-values (group-by
                        :enum
                        (jdbc/query con [(str "SELECT t.typname as \"enum\", e.enumlabel as value "
                                              "  FROM pg_type t "
                                              "  JOIN pg_enum e ON t.oid = e.enumtypid "
                                              "  JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace")]))]
       (spit schema-file
             (pr-str
              {:enums (into {} (map (fn [[enum vals]]
                                      [enum (into #{} (map :value) vals)])
                                    enum-values))
               :tables (into {}
                             (for [{n :relname} (jdbc/query con [(str "SELECT relname FROM pg_catalog.pg_class "
                                                                      " WHERE relkind IN ('r','c','v','m')"
                                                                      "   AND relname NOT LIKE 'pg_%'")])]
                               [n (table-info* s n)]))
               :sprocs (into {}
                             (for [{n :proname} (jdbc/query con [(str "SELECT proname"
                                                                      "  FROM pg_catalog.pg_proc p "
                                                                      "  JOIN pg_catalog.pg_namespace n ON p.pronamespace = n.oid "
                                                                      " WHERE nspname != 'pg_catalog' "
                                                                      "   AND probin IS NULL"
                                                                      "   AND proname NOT LIKE '_pg_%'")])]
                               [n (sproc-info* s n)]))}))))))
