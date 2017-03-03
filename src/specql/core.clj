(ns specql.core
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.spec :as s]
            [specql.data-types :as d]
            [clojure.string :as str]))

(s/def ::column-info (s/keys :req [::column-name ::data-type ::table-name]))

(def relkind-q "SELECT relkind FROM pg_catalog.pg_class WHERE relname=?")
(def columns-q (str "SELECT attr.attname AS name, attr.attnum AS number,"
                    "attr.attnotnull AS \"not-null?\","
                    "t.typname AS type "
                    "FROM pg_catalog.pg_attribute attr "
                    "JOIN pg_catalog.pg_class cls ON attr.attrelid = cls.oid "
                    "JOIN pg_catalog.pg_type t ON attr.atttypid = t.oid "
                    "WHERE cls.relname = ? AND attnum > 0"))

(defn- table-info [db name]
  (let [relkind (-> (jdbc/query db [relkind-q name])
                    first :relkind)
        columns (jdbc/query db [columns-q name])]
    {:name name
     :type (case relkind
             "c" :composite
             "r" :table)
     :columns (into {}
                    (map (juxt :name identity))
                    columns)})
  #_(jdbc/query db ["SELECT column_name, data_type, is_identity, is_nullable FROM information_schema.columns WHERE table_name=?" name]))


(defonce table-info-registry (atom {}))

(defn- process-columns [{columns :columns :as table-info} ns-name]
  (assoc table-info
         :columns (reduce-kv (fn [columns name column]
                               (assoc columns
                                      (keyword ns-name name)
                                      column))
                             {} columns)))

(defn- composite-type
  "Find user defined composite type from registry by name."
  [name]
  (some (fn [[key {n :name t :type}]]
          (and (= :composite t)
               (= name n)
                key))
        @table-info-registry))

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
  (let [db (eval db)]

    (doseq [[name kw] tables]
      (swap! table-info-registry
             assoc kw (table-info db name)))
    (let [table-info (into {}
                           (map (fn [[table-name table-keyword]]
                                  [table-keyword
                                   (-> (table-info db table-name)
                                       (process-columns (name (namespace table-keyword))))]))
                           tables)]
      (swap! table-info-registry merge table-info)
      `(do
         ~@(for [[table-keyword {columns :columns :as table}] table-info]
             `(do
                ;; Create the keys spec for the table
                (s/def ~table-keyword
                  (s/keys :opt [~@(keys columns)]))

                ;; Create specs for columns
                ~@(for [[kw {type :type :as column}] columns
                        :let [type-spec (case type
                                          "int4" ::d/integer
                                          "varchar" ::d/text
                                          (composite-type type))]
                        :when type-spec]
                    `(s/def ~kw ~type-spec))))))))

#_(define-tables db
  ["department" :department/departments])

#_(fetch :department/departments
       #{:department/id :department/name [:department/parent :department/name]}
       (match :deparment/id))

(defn assert-spec
  "Unconditionally assert that value is valid for spec. Returns value."
  [spec value]
  (assert (s/valid? spec value)
          (s/explain-str spec value))
  value)

(defmacro fetch [db table columns where]
  (let [{table-name :name table-columns :columns :as table-info}
        (@table-info-registry table)]
    (assert table-info (str "Unknown table " table ", call define-tables!"))
    (let [alias (gensym (subs table-name 0 2))
          cols (reduce (fn [cols column-kw]
                         (let [name (:name (table-columns column-kw))]
                           (assert name (str "Unknown column " column-kw " for table " table))
                           (println "COLS: " cols)
                           (assoc cols
                                  (keyword (gensym (subs name 0 2)))
                                  [name column-kw])))
                       {} columns)

          where (map (fn [[column-keyword value-form]]
                       (let [col-name (:name (column-keyword table-columns))]
                         [col-name
                          (gensym (subs col-name 0 1))
                          `(assert-spec ~column-keyword ~value-form)]))
                     where)
          sql (str "SELECT "
                   (str/join ", "
                             (map (fn [[col-alias [col-name _]]]
                                    (str alias "." col-name " AS " (name col-alias)))
                                  cols))
                   " FROM " table-name " " alias
                   " WHERE "
                   (str/join " AND "
                             (map (comp #(str alias "." % " = ?") first)
                                  where)))
          row (gensym "row")]
      `(let [~@(mapcat (fn [[_ sym value-form]]
                         [sym value-form])
                       where)]
         (map
            ;; Generate a row processing function that turns the column aliases
            ;; to the namespaced keys we want.
            (fn [~row]
              (hash-map
               ~@(mapcat
                  (fn [[resultset-kw [_ output-kw]]]
                    ;; PENDING: read composite record types here
                    ;; or create columns that access all subfields
                    [output-kw (list resultset-kw row)])
                  cols)))

            ;; Query the generated SQL with the where map arguments
            (jdbc/query ~db [~sql ~@(map second where)]))))))
