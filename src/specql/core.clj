(ns specql.core
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.spec :as s]
            [specql.data-types :as d]
            [clojure.string :as str]
            [specql.op :as op]
            [specql.rel :as rel]))

;; Extend java.util.Date to be a SQL date parameter
(extend-protocol jdbc/ISQLValue
  java.util.Date
  (sql-value [dt]
    (java.sql.Date. (.getTime dt))))

(s/def ::column-info (s/keys :req [::column-name ::data-type ::table-name]))

(def relkind-q "SELECT relkind FROM pg_catalog.pg_class WHERE relname=?")
(def columns-q (str "SELECT attr.attname AS name, attr.attnum AS number,"
                    "attr.attnotnull AS \"not-null?\","
                    "attr.atthasdef AS \"has-default?\","
                    "t.typname AS type, "
                    "EXISTS(SELECT indkey FROM pg_catalog.pg_index i "
                    "        WHERE i.indrelid=attr.attrelid AND attr.attnum=ANY(i.indkey) AND i.indisprimary=TRUE) AS \"primary-key?\","
                    "EXISTS(SELECT e.enumtypid FROM pg_catalog.pg_enum e WHERE e.enumtypid = t.oid) AS \"enum?\""
                    "FROM pg_catalog.pg_attribute attr "
                    "JOIN pg_catalog.pg_class cls ON attr.attrelid = cls.oid "
                    "JOIN pg_catalog.pg_type t ON attr.atttypid = t.oid "
                    "WHERE cls.relname = ? AND attnum > 0"))
(def enum-values-q (str "SELECT e.enumlabel AS value"
                        "  FROM pg_type t"
                        "       JOIN pg_enum e ON t.oid = e.enumtypid"
                        "       JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace"
                        " WHERE t.typname=?"))

(defn- enum-values [db enum-type-name]
  (into #{}
        (map :value)
        (jdbc/query db [enum-values-q enum-type-name])))

(defn- table-info [db name]
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
                 "r" :table)
         :columns (into {}
                        (map (juxt :name identity))
                        columns)}))))


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

(defn- enum-type
  "Find an enum type from registry by name."
  ([name] (enum-type @table-info-registry name))
  ([table-info-registry name]
   (some (fn [[key {n :name t :type}]]
           (and (= :enum t)
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
                           (map (fn [[table-name table-keyword rel]]
                                  (let [ns (name (namespace table-keyword))]
                                    [table-keyword
                                     (-> (table-info db table-name)
                                         (assoc :insert-spec-kw
                                                (keyword ns (str (name table-keyword) "-insert")))
                                         (process-columns ns)
                                         (assoc :rel rel))])))
                           tables)]

      `(do
         ;; Register table info so that it is available at runtime
         (swap! table-info-registry merge ~table-info)
         ~@(for [[table-keyword {columns :columns
                                 insert-spec-kw :insert-spec-kw :as table}] table-info
                 :let [{required-insert true
                        optional-insert false} (group-by (comp required-insert? second) columns)]]
             (if (= :enum (:type table))
               `(s/def ~table-keyword ~(:values table))
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
                          :let [type-spec (or (composite-type table-info type)
                                              (and (:enum? column)
                                                   (or
                                                    ;; previously registered enum type
                                                    (enum-type table-info type)
                                                    ;; just the values as spec
                                                    (enum-values db type)))
                                              (keyword "specql.data-types" type))]
                          :when type-spec]
                      `(s/def ~kw ~type-spec)))))))))

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
                  (keyword (gensym (subs name 0 1)))
                  [(str table-alias ".\"" name "\"") column-kw]))))
     {} columns-set)))

(defn sql-columns-list [cols]
  (str/join ", "
            (map (fn [[col-alias [col-name _]]]
                   (str col-name " AS " (name col-alias)))
                 cols)))

(defn- sql-where [table-info-registry table alias record]
  (if (op/combined-op? record)
    ;; OR/AND on the top level
    (let [{combine-with :combine-with records :ops} record]
      (loop [sql []
             params []
             [record & records] records]
        (if-not record
          [(str "(" (str/join combine-with sql) ")")
           params]
          (let [[record-sql record-params]
                (sql-where table-info-registry table alias record)]
            (recur (conj sql record-sql)
                   (into params record-params)
                   records)))))
    ;; Regular map of field to value
    (let [table-columns (-> table table-info-registry :columns)
          add-where (fn [{:keys [:clause :parameters] :as where} column-accessor column-keyword value]
                      (if (satisfies? op/Op value)
                        ;; This is an operator, call to-sql to create SQL clause and params
                        (let [[cl params] (op/to-sql value column-accessor)]
                          (assoc where
                                 :clause (conj clause cl)
                                 :parameters (into parameters params)))
                        ;; Plain old value, assert that it is valid and create = comparison
                        (assoc where
                               :clause (conj clause (str column-accessor " = ?"))
                               :parameters (conj parameters (assert-spec column-keyword value)))))]
      (as-> (reduce
             (fn [where [column-keyword value]]
               (let [{col-name :name :as column} (column-keyword table-columns)]
                 (if-let [composite-columns (some->> column :type
                                                     (composite-type table-info-registry)
                                                     table-info-registry
                                                     :columns)]
                   ;; composite type: add all fields as separate clauses
                   (reduce (fn [where [kw val]]
                             (add-where where
                                        (str "(" alias ".\"" col-name "\").\""
                                             (:name (composite-columns kw)) "\"")
                                        kw val))
                           where value)

                   ;; normal column
                   (add-where where (str alias ".\"" col-name "\"") column-keyword value))))
             {:clause [] :parameters []}
             record) w
        (update w :clause #(str/join " AND " %))
        ((juxt :clause :parameters) w)))))

(defn- gen-alias []
  (let [alias (volatile! 0)]
    (fn [named]
      (let [n (name named)]
        (str (subs n 0 (min (count n) 3))
             (vswap! alias inc))))))

(defn- fetch-tables
  "Determine all tables to query from. JOINs tables indicated by columns.
  Returns a vector tables and aliases with join clauses:
  [[:main/table \"mai1\" nil nil #{...cols for main table...}]
   [:other/table \"oth2\" :left [\"mai1.other = oth2.id\"] #{...cols for other table...}]]"
  [table-info-registry alias-fn primary-table columns]
  (let [{primary-table-columns :columns :as primary-table-info}
        (table-info-registry primary-table)
        primary-table-alias (alias-fn primary-table)]
    (assert primary-table-info
            (str "Unknown table " primary-table ", call define-tables!"))
    (loop [;; start with the primary table and all fields that are not joins
           table [[primary-table primary-table-alias nil nil
                   (into #{}
                         (remove vector?)
                         columns)]]
           ;; go throuh all JOINS
           [c & cs] (filter vector? columns)]
      (if-not c
        ;; No more joins, return accumulated tables
        table

        ;; Join table
        (let [[join-field join-table-columns] c
              rel (-> table-info-registry
                      primary-table
                      :rel join-field)
              this-table-column
              (-> rel ::rel/this-table-column primary-table-columns)
              join-type (if (:not-null? this-table-column)
                          ;; NOT NULL field, do an inner join (pg default join)
                          :join
                          ;; May be NULL, do a left join
                          :left-join)
              other-table-alias (alias-fn (::rel/other-table rel))
              other-table-columns (-> rel ::rel/other-table table-info-registry :columns)
              other-table-column (-> rel ::rel/other-table-column other-table-columns)]
          (recur (conj table
                       [(::rel/other-table rel)
                        other-table-alias
                        join-type
                        (str primary-table-alias ".\""
                             (:name this-table-column) "\" = "
                             other-table-alias ".\""
                             (:name other-table-column) "\"")
                        join-table-columns])
                 cs))))))
(defn fetch [db table columns where]
  (let [table-info-registry @table-info-registry
        {table-name :name table-columns :columns :as table-info}
        (table-info-registry table)]
    (assert table-info (str "Unknown table " table ", call define-tables!"))
    (let [alias-fn (gen-alias)
          alias (gensym (subs table-name 0 1))
          table-alias (fetch-tables table-info-registry alias-fn table columns)
          cols (fetch-columns table-info-registry table alias columns)

          [where-clause where-parameters]
          (sql-where table-info-registry table alias where)

          sql (str "SELECT "
                   (sql-columns-list cols)
                   " FROM " table-name " " alias
                   (when-not (str/blank? where-clause)
                     (str " WHERE " where-clause)))
          row (gensym "row")]
      ;(println "SQL: " (pr-str (into [sql] where-parameters)))
      (map
       ;; Process each row and remap the columns
       ;; to the namespaced keys we want.
       (fn [resultset-row]
         (reduce (fn [row [resultset-kw [_ output-path]]]
                   (let [v (resultset-kw resultset-row)]
                     (if (nil? v)
                       row
                       (if (keyword? output-path)
                         (assoc row output-path v)
                         (assoc-in row output-path v)))))
                 {}
                 cols))

       ;; Query the generated SQL with the where map arguments
       (jdbc/query db (into [sql] where-parameters))))))

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

            (if-let [enum-type-kw (enum-type table-info-registry (:type column))]
              ;; Enum type, add value with ::enumtype cast
              (recur (conj names (:name column))
                     (conj value-names (str "?::" (:type column)))
                     (conj value-parameters value)
                     columns)

              ;; Normal value, add name and value
              (recur (conj names (:name column))
                     (conj value-names "?")
                     (conj value-parameters value)
                     columns))))))))

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
          cols (when-not (empty? primary-key-columns)
                 (fetch-columns table-info-registry table-kw alias primary-key-columns))
          [column-names value-names value-parameters]
          (insert-columns-and-values table-info-registry table-kw record)

          sql (str "INSERT INTO " table-name " AS " alias " ("
                   (str/join ", " (map #(str "\"" % "\"") column-names)) ") "
                   "VALUES (" (str/join "," value-names) ") "
                   (when cols
                     (str "RETURNING " (sql-columns-list cols))))
          sql-and-params (into [sql] value-parameters)]
      ;(println "SQL: " (pr-str sql-and-params))
      (if (empty? primary-key-columns)
        (do (jdbc/execute! db sql-and-params)
            record)
        (let [result (first (jdbc/query db sql-and-params))]
          (reduce (fn [record [resultset-kw [_ output-kw]]]
                    (assoc record output-kw (result resultset-kw)))
                  record
                  cols))))))
