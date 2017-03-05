(ns specql.core
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.spec :as s]
            [specql.data-types :as d]
            [clojure.string :as str]))

(s/def ::column-info (s/keys :req [::column-name ::data-type ::table-name]))

(def relkind-q "SELECT relkind FROM pg_catalog.pg_class WHERE relname=?")
(def columns-q (str "SELECT attr.attname AS name, attr.attnum AS number,"
                    "attr.attnotnull AS \"not-null?\","
                    "attr.atthasdef AS \"has-default?\","
                    "t.typname AS type, "
                    "EXISTS(SELECT indkey FROM pg_catalog.pg_index i "
                    "        WHERE i.indrelid=attr.attrelid AND attr.attnum=ANY(i.indkey) AND i.indisprimary=TRUE) AS \"primary-key?\""
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
  ([name] (composite-type @table-info-registry name))
  ([table-info-registry name]
   (some (fn [[key {n :name t :type}]]
           (and (= :composite t)
                (= name n)
                key))
         table-info-registry)))

(defn- required-insert? [{:keys [not-null? has-default?]}]
  (and not-null?
       (not has-default?)))

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
    (let [table-info (into {}
                           (map (fn [[table-name table-keyword]]
                                  (let [ns (name (namespace table-keyword))]
                                    [table-keyword
                                     (-> (table-info db table-name)
                                         (assoc :insert-spec-kw
                                                (keyword ns (str (name table-keyword) "-insert")))
                                         (process-columns ns))])))
                           tables)]

      `(do
         ;; Register table info so that it is available at runtime
         (swap! table-info-registry merge ~table-info)
         ~@(for [[table-keyword {columns :columns
                                 insert-spec-kw :insert-spec-kw :as table}] table-info
                 :let [{required-insert true
                        optional-insert false} (group-by (comp required-insert? second) columns)]]
             `(do
                ;; Create the keys spec for the table
                (s/def ~table-keyword
                  (s/keys :opt [~@(keys columns)]))

                ;; Spec for a "full" row that has all NOT NULL values
                (s/def ~insert-spec-kw
                  (s/keys :req [~@(keys required-insert)]
                          :opt [~@(keys optional-insert)]))

                ;; Create specs for columns
                ~@(for [[kw {type :type :as column}] columns
                        :let [type-spec (case type
                                          "int4" ::d/integer
                                          "varchar" ::d/text
                                          ;; FIXME: moar types!
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

(defn fetch-columns
  "Return a map of column alias to vector of [sql-accessor result-path]"
  [table-info-registry table table-alias columns-set]
  (let [table-columns (:columns (table-info-registry table))]
    (reduce
     (fn [cols column-kw]
       (let [name (:name (table-columns column-kw))
             composite-type-kw (composite-type name)]
         (assert name (str "Unknown column " column-kw " for table " table))
         (if composite-type-kw
           ;; This field is a composite type, add "(field).subfield" accessors
           ;; for each field in the type
           (merge cols
                  (into {}
                        (map (fn [[field-kw field]]
                               [(keyword (gensym (subs name 0 2)))
                                [(str "(" table-alias ".\"" name "\").\"" (:name field) "\"")
                                 [column-kw field-kw]]]))
                        (:columns (table-info-registry composite-type-kw))))
           ;; A regular field
           (assoc cols
                  (keyword (gensym (subs name 0 2)))
                  [(str table-alias ".\"" name "\"") column-kw]))))
     {} columns-set)))

(defn sql-columns-list [cols]
  (str/join ", "
            (map (fn [[col-alias [col-name _]]]
                   (str col-name " AS " (name col-alias)))
                 cols)))

(defn fetch [db table columns where]
  (let [table-info-registry @table-info-registry
        {table-name :name table-columns :columns :as table-info}
        (table-info-registry table)]
    (assert table-info (str "Unknown table " table ", call define-tables!"))
    (let [alias (gensym (subs table-name 0 2))
          cols (fetch-columns table-info-registry table alias columns)

          where (map (fn [[column-keyword value-form]]
                       (let [col-name (:name (column-keyword table-columns))]
                         [col-name
                          (assert-spec column-keyword value-form)]))
                     where)
          sql (str "SELECT "
                   (sql-columns-list cols)
                   " FROM " table-name " " alias
                   (when-not (empty? where)
                     (str " WHERE "
                          (str/join " AND "
                                    (map (comp #(str alias "." % " = ?") first)
                                         where)))))
          row (gensym "row")]
      ;(println "SQL: " sql)
      (map
       ;; Process each row and remap the columns
       ;; to the namespaced keys we want.
       (fn [resultset-row]
         (reduce (fn [row [resultset-kw [_ output-path]]]
                   (let [v (resultset-kw resultset-row)]
                     (if (keyword? output-path)
                       (assoc row output-path v)
                       (assoc-in row output-path v))))
                 {}
                 cols))

       ;; Query the generated SQL with the where map arguments
       (jdbc/query db (into [sql] (map second) where))))))

(defn- insert-columns-and-values [table-info-registry table record]
  (let [table-columns (:columns (table-info-registry table))]
    (loop [names []
           value-names []
           value-parameters []
           [[column-kw value] & columns] (seq record)]
      (let [column (table-columns column-kw)]
        (if-not column
          [names value-names value-parameters]
          (if-let [composite-type-kw (composite-type table-info-registry (:type column))]
            ;; This is a composite type, add ROW(?,...)::type value
            (let [composite-type (table-info-registry composite-type-kw)
                  composite-columns (:columns composite-type)]
              (recur (conj names (:name column))
                     (conj value-names
                           (str "ROW("
                                (str/join "," (repeat (count composite-columns) "?"))
                                ")::"
                                (:name composite-type)))
                     ;; Get all values in order and add to parameter value
                     (into value-parameters
                           (map (comp (partial get value) first)
                                (sort-by (comp :number second) composite-columns)))
                     columns))
            ;; Normal value, add name and value
            (recur (conj names (:name column))
                   (conj value-names "?")
                   (conj value-parameters value)
                   columns)))))))

(defn insert!
  "Insert a record to the given table. Returns the inserted record with the
  (possibly generated) primary keys added."
  [db table-kw record]
  (let [table-info-registry @table-info-registry
        {table-name :name columns :columns
         insert-spec :insert-spec-kw :as table}
        (table-info-registry table-kw)]
    (assert table (str "Unknown table " table ", call define-tables!"))
    (assert-spec insert-spec record)
    (let [primary-key-columns (into #{}
                                    (keep (fn [[kw {pk? :primary-key?}]]
                                            (when pk?
                                              kw))
                                          columns))
          alias (gensym "ins")
          cols (fetch-columns table-info-registry table-kw alias primary-key-columns)
          [column-names value-names value-parameters]
          (insert-columns-and-values table-info-registry table-kw record)

          sql (str "INSERT INTO " table-name " AS " alias " ("
                   (str/join ", " column-names) ") "
                   "VALUES (" (str/join "," value-names) ") "
                   "RETURNING " (sql-columns-list cols))
          sql-and-params (into [sql] value-parameters)]
      (let [result (first (jdbc/query db sql-and-params))]
        (reduce (fn [record [resultset-kw [_ output-kw]]]
                  (assoc record output-kw (result resultset-kw)))
                record
                cols)))))
