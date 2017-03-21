(ns specql.core
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.spec :as s]
            [specql.data-types :as d]
            [clojure.string :as str]
            [specql.op :as op]
            [specql.rel :as rel]
            [specql.impl.catalog :refer :all]
            [specql.impl.registry :as registry :refer :all]
            [specql.impl.composite :as composite]))

;; Remove this when 1.9 is out
(when-not (resolve 'any?)
  (require '[clojure.future :refer :all]))

;; Extend java.util.Date to be a SQL timestamp parameter
(extend-protocol jdbc/ISQLValue
  java.util.Date
  (sql-value [dt]
    (java.sql.Timestamp. (.getTime dt))))


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
                           tables)
          new-table-info (reduce-kv
                          (fn [m k v]
                            (assoc m k
                                   (update v :columns
                                           (fn [columns]
                                             (reduce-kv
                                              (fn [cols key val]
                                                (assoc cols key
                                                       (registry/array-element-type table-info val)))
                                              {}
                                              columns)))))
                          {}
                          table-info)
          table-info (merge @table-info-registry new-table-info)]

      `(do
         ;; Register table info so that it is available at runtime
         (swap! table-info-registry merge ~new-table-info)
         ~@(for [[_ table-keyword] tables
                 :let [{columns :columns
                        insert-spec-kw :insert-spec-kw :as table}
                       (table-info table-keyword)
                       {required-insert true
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
                  ~@(for [[kw {type :type
                               category :category
                               :as column}] columns
                          :let [array? (= "A" category)
                                type (if array?
                                       (subs type 1)
                                       type)
                                type-spec
                                (if array?
                                  (:element-type column)
                                  (or (composite-type table-info type)
                                      (and (:enum? column)
                                           (or
                                            ;; previously registered enum type
                                            (enum-type table-info type)
                                            ;; just the values as spec
                                            (enum-values db type)))
                                      (keyword "specql.data-types" type)))]
                          :when type-spec]
                      `(s/def ~kw ~(if array?
                                     `(s/coll-of ~type-spec)
                                     type-spec))))))))))

(defn- assert-spec
  "Unconditionally assert that value is valid for spec. Returns value."
  [spec value]
  (assert (s/valid? spec value)
          (s/explain-str spec value))
  value)

(defn- assert-table [table]
  (assert (@table-info-registry table)
          (str "Unknown table " table ", call define-tables!")))

(defn- q
  "Surround string with doublequotes"
  [string]
  (str "\"" string "\""))

(defn- fetch-columns
  "Return a map of column alias to vector of [sql-accessor result-path]"
  [table-info-registry table table-alias alias-fn column->path]
  (let [table-columns (:columns (table-info-registry table))]
    (reduce
     (fn [cols [column-kw result-path]]
       (let [col (table-columns column-kw)
             name (:name col)
             composite-type-kw (composite-type (:type col))]
         (assert name (str "Unknown column " column-kw " for table " table))
         (if composite-type-kw
           ;; This field is a composite type, add "(field).subfield" accessors
           ;; for each field in the type
           (merge cols
                  (into {}
                        (map (fn [[field-kw field]]
                               [(keyword (alias-fn name))
                                [(str "(" table-alias ".\"" name "\").\"" (:name field) "\"")
                                 (into result-path [field-kw])
                                 col]]))
                        (:columns (table-info-registry composite-type-kw))))
           ;; A regular field
           (assoc cols
                  (keyword (alias-fn name))
                  [(str table-alias ".\"" name "\"") result-path col]))))
     {} column->path)))

(defn- sql-columns-list [cols]
  (str/join ", "
            (map (fn [[col-alias [col-name _]]]
                   (str col-name " AS " (name col-alias)))
                 cols)))

(defn- sql-where
  ([table-info-registry path->table record]
   (sql-where table-info-registry path->table record []))
  ([table-info-registry path->table record path-prefix]
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
                 (sql-where table-info-registry path->table record path-prefix)]
             (recur (conj sql record-sql)
                    (into params record-params)
                    records)))))
     ;; Regular map of field to value
     (let [{:keys [table alias]} (path->table path-prefix)
           table-columns (-> table table-info-registry :columns)
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
                ;; If column is a joined table, it has a mapping in path->table.
                ;; Recursively create clauses for the value
                (if (path->table (into path-prefix [column-keyword]))
                  (let [[sql params] (sql-where table-info-registry path->table value
                                                (into path-prefix [column-keyword])) ]
                    (assoc where
                           :clause (conj (:clause where) (str "(" sql ")"))
                           :parameters (into (:parameters where) params)))

                  ;; This is a column in the current table
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
                      (add-where where (str alias ".\"" col-name "\"") column-keyword value)))))
              {:clause [] :parameters []}
              record) w
         (update w :clause #(str/join " AND " %))
         ((juxt :clause :parameters) w))))))

(defn- gen-alias
  "Returns a function that create successively numbered aliases for keywords."
  []
  (let [alias (volatile! 0)]
    (fn [named]
      (let [n (name named)]
        (str (subs n 0 (min (count n) 3))
             (vswap! alias inc))))))

(defn- fetch-tables
  "Determine all tables to query from. JOINs tables indicated by columns.
  Returns a vector tables and aliases with join clauses and column mapping:
  [[:main/table \"mai1\" nil nil {:main/column [:main/column]}]
   [:other/table \"oth2\" :left [\"mai1.other = oth2.id\"] {:other/id [:main/other :other/id]} has-many-collection]]"
  ([table-info-registry alias-fn primary-table primary-table-alias columns]
   (fetch-tables table-info-registry alias-fn primary-table primary-table-alias columns true []))
  ([table-info-registry alias-fn primary-table primary-table-alias columns
    add-primary-table? path-prefix]
   (let [{primary-table-columns :columns :as primary-table-info}
         (table-info-registry primary-table)]
     (assert primary-table-info
             (str "Unknown table " primary-table ", call define-tables!"))
     (loop [;; start with the primary table and all fields that are not joins
            table (when add-primary-table?
                    [[primary-table primary-table-alias nil nil
                      (into {}
                            (comp (remove vector?)
                                  (map (juxt identity vector)))
                            columns)]])
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
               join-type (if (and (not= ::rel/has-many
                                        (::rel/type rel))
                                  (:not-null? this-table-column))
                           ;; NOT NULL field, do an inner join (pg default join)
                           :join
                           ;; May be NULL, do a left join
                           :left-join)
               other-table-alias (alias-fn (::rel/other-table rel))
               other-table-columns (-> rel ::rel/other-table table-info-registry :columns)

               other-table-column (-> rel ::rel/other-table-column other-table-columns)
               nested-joins (when (some vector? join-table-columns)
                              (fetch-tables table-info-registry
                                            alias-fn (::rel/other-table rel) other-table-alias
                                            join-table-columns false
                                            (conj path-prefix join-field)))]
           (recur (into table
                        (concat
                         [[(::rel/other-table rel)
                            other-table-alias
                            join-type
                            (str primary-table-alias ".\""
                                 (:name this-table-column) "\" = "
                                 other-table-alias ".\""
                                 (:name other-table-column) "\"")
                           (into {}
                                  (map (juxt identity
                                             (fn [c]
                                               (into path-prefix
                                                     (conj [join-field] c)))))
                                  (remove vector? join-table-columns))
                           (when (= ::rel/has-many (::rel/type rel))
                             (into path-prefix [join-field]))]]
                         nested-joins))
                  cs)))))))

(defn- sql-from [table-info-registry tables]
  (str/join
   " "
   (for [[table alias join-type join-cond _] tables]
     (str (case join-type
            :join " JOIN "
            :left-join " LEFT JOIN "
            "")
          "\"" (:name (table-info-registry table)) "\" " alias
          (when join-cond
            (str " ON " join-cond))))))

(defn- path->table-mapping [table-alias]
  (into {}
        (mapcat (fn [[table alias _ _ paths]]
                  (for [[_ p] paths]
                    [(subvec p 0 (dec (count p)))
                     {:table table
                      :alias alias}])))
        table-alias))




(defn- collection-processing-fn
  "Takes columns describing has-many joins and returns a grouping function
  that is used to determine row ::group metadata and a function that processes
  all rows and combines results."
  [has-many-join-cols]
  (let [group-fn (apply juxt (map first has-many-join-cols))
        last-idx (dec (count has-many-join-cols))
        vectorize (fn [row]
                    ;; Make sure collections are vectors
                    (reduce (fn [row [_ [_ path]]]
                              (update-in row path
                                         #(if (vector? %) % [%])))
                            row
                            has-many-join-cols))]
    [group-fn
     (fn [results]
       ;; PENDING: Make this a lazy operation
       (loop [acc []
              previous-row (first results)
              previous-group (::group (meta previous-row))
              [row & rows] (rest results)]
         (if-not row
           (if (nil? previous-row)
             acc
             (seq (conj acc (vectorize previous-row))))
           (let [row-group (::group (meta row))]
             ;; Go through the group in reverse order, if a ctid is changed
             ;; add to the corresponding collection.
             ;; If the 0th ctid has changed, emit a new result row
             (if-let [combined-row
                      (loop [i last-idx]

                        (if (neg? i)
                          ;; No same ctids found, emit new row
                          nil
                          (if (= (nth previous-group i)
                                 (nth row-group i))
                            (let [[_ collection] (second (nth has-many-join-cols i))
                                  next-val (get-in row collection)]
                              (update-in previous-row collection
                                         (fn [val]
                                           (if (vector? val)
                                             (conj val next-val)
                                             [val next-val]))))
                            (recur (dec i)))))]
               (recur acc
                      combined-row row-group
                      rows)

               (recur (conj acc
                            (vectorize previous-row))
                      row row-group
                      rows))))))]))

(defn- has-many-join-columns
  "If there are many-to-many joins, add postgres physical row identity field to the
  result so that we can use it to detect unique rows.
  The resultset processing will use the ctid fields to detect same rows and
  add nested maps to the correct sequences."
  [table-alias alias-fn]
  (vec
   (keep (fn [[left right]]
           ;; If the right table is a has-many join to the left,
           ;; add the left table ctid
           (when-let [has-many-collection (nth right 5)]
             [(keyword (alias-fn :ctid))
              [(str "CAST(\"" (second left) "\".ctid AS varchar)")
               ;; the collection to add to if this id hasn't changed
               has-many-collection]]))
         (partition 2 1 table-alias))))

(defn- post-process-arrays-fn [path->array-type]
  (let [tir @table-info-registry]
    (fn [results]
      (doall
       (map
        (fn [row]
          (reduce
           (fn [row [path array-type]]
             ;; We need to omit empty array keys
             (let [parse-composite (fn [arr]
                                     (let [string (str arr)]
                                       (if (str/blank? string)
                                         nil
                                         (composite/parse tir array-type string))))
                   update-composite (fn [m k]
                                      (let [c (some-> (get m k) parse-composite)]
                                        (if (nil? c)
                                          (dissoc m k)
                                          (assoc m k c))))]
               (cond
                 (and (> (count path) 1)
                      (vector? (get-in row (subvec path 0 (dec (count path))))))
                 ;; This is a collection of joined values, do update for all of them
                 ;; PENDING: detect this in a better way
                 (update-in row (subvec path 0 (dec (count path)))
                            (fn [items]
                              (mapv #(update-composite % (last path)) items)))

                 (= 1 (count path))
                 (update-composite row (first path))

                 :else
                 (update-in row (subvec path 0 (dec (count path)))
                            #(update-composite % (last path))))))
           row
           path->array-type))
        results)))))

(defn fetch [db table columns where]
  (assert-table table)
  (let [table-info-registry @table-info-registry
        {table-name :name table-columns :columns :as table-info}
        (table-info-registry table)
        alias-fn (gen-alias)
        alias (alias-fn table)
        table-alias (fetch-tables table-info-registry alias-fn
                                  table alias columns)

        cols (reduce merge
                     {}
                     (for [[table alias _ _ columns] table-alias]
                       (fetch-columns table-info-registry table alias alias-fn columns)))

        has-many-join-cols (has-many-join-columns table-alias alias-fn)

        [group-fn process-collections]
        (if (empty? has-many-join-cols)
          ;; No has-many joins, don't do collection processing
          [nil identity]

          ;; Create a function to add rows to collections
          (collection-processing-fn has-many-join-cols))

        path->table (path->table-mapping table-alias)
        [where-clause where-parameters]
        (sql-where table-info-registry path->table where)

        all-cols (into cols has-many-join-cols)
        sql (str "SELECT " (sql-columns-list all-cols)
                 " FROM " (sql-from table-info-registry table-alias)
                 (when-not (str/blank? where-clause)
                   (str " WHERE " where-clause)))
        row (gensym "row")
        sql-and-parameters (into [sql] where-parameters)

        array-paths (into {}
                          (keep (fn [[_ [_ path col]]]
                                  (when (= "A" (:category col))
                                    [path col])))
                          cols)

        process-collections (if (empty? array-paths)
                              process-collections
                              (comp (post-process-arrays-fn array-paths)
                                    process-collections))]

    ;;(println "cols: " (pr-str cols))
    ;;(println "SQL: " (pr-str sql-and-parameters))

    ;; Post process: parse arrays after joined collections
    ;; have been processed. So that we don't unnecessarily parse
    ;; the same array many times
    (jdbc/with-db-connection [db db]
      (with-meta
        (process-collections
         (map
          ;; Process each row and remap the columns
          ;; to the namespaced keys we want.
          (fn [resultset-row]
            (with-meta
              (reduce
               (fn [row [resultset-kw [_ output-path]]]
                 (let [v (resultset-kw resultset-row)]
                   (if (nil? v)
                     row
                     (assoc-in row output-path v))))
               {}
               cols)
              (when group-fn
                {::group (group-fn resultset-row)})))

          ;; Query the generated SQL with the where map arguments
          (jdbc/query db sql-and-parameters)))
        {::sql sql-and-parameters}))))

(defn- insert-columns-and-values [table-info-registry table record]
  (let [table-columns (:columns (table-info-registry table))]
    (loop [names []
           value-names []
           value-parameters []
           [[column-kw value] & columns] (seq record)]
      (if-not column-kw
        [names value-names value-parameters]
        (let [column (table-columns column-kw)]
          (assert column (str "Unknown column " (pr-str column-kw) " for table " (pr-str table)))
          (if (= "A" (:category column))
            ;; This is an array, serialize it
            (recur (conj names (:name column))
                   (conj value-names (str "?::" (subs (:type column) 1) "[]"))
                   (conj value-parameters (composite/stringify table-info-registry column value true))
                   columns)

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
                       columns)))))))))

(defn- primary-key-columns [columns]
  (into {}
        (keep (fn [[kw {pk? :primary-key?}]]
                (when pk?
                  [kw [kw]]))
              columns)))

(defn insert!
  "Insert a record to the given table. Returns the inserted record with the
  (possibly generated) primary keys added."
  [db table-kw record]
  (assert-table table-kw)
  (let [table-info-registry @table-info-registry
        {table-name :name columns :columns
         insert-spec :insert-spec-kw :as table}
        (table-info-registry table-kw)]
    (assert-spec insert-spec record)
    (let [primary-key-columns (primary-key-columns columns)
          alias-fn (gen-alias)
          alias (alias-fn :ins)
          cols (when-not (empty? primary-key-columns)
                 (fetch-columns table-info-registry table-kw alias alias-fn primary-key-columns))
          [column-names value-names value-parameters]
          (insert-columns-and-values table-info-registry table-kw record)

          sql (str "INSERT INTO \"" table-name "\" AS " alias " ("
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
                    (assoc-in record output-kw (result resultset-kw)))
                  record
                  cols))))))

(defn delete!
  "Delete rows from table that match the given search criteria.
  Returns the number of rows deleted."
  [db table where]
  (assert-table table)
  (let [table-info-registry @table-info-registry
        {table-name :name} (table-info-registry table)
        alias-fn (gen-alias)
        alias (alias-fn table)
        [where-clause where-parameters]
        (sql-where table-info-registry
                   #(when (= % [])
                      {:table table
                       :alias alias})
                   where)
        sql (str "DELETE FROM " table-name " AS " alias
                 " WHERE " where-clause)]
    (assert (not (str/blank? where-clause))
            "Will not delete with an empty where clause")
    (first
     (jdbc/execute! db
                    (into [sql] where-parameters)))))

(defn update! [db table record where]
  (assert-table table)
  (let [table-info-registry @table-info-registry
        {table-name :name columns :columns} (table-info-registry table)
        alias-fn (gen-alias)
        alias (alias-fn table-name)

        cols (map (juxt (comp :name columns first)
                        second)
                  record)

        _ (assert (every? (comp some? first) cols)
                  (str "Unknown columns in update: "
                       (into #{}
                             (comp
                              (map first)
                              (remove #(contains? columns %)))
                             record)))
        [where-clause where-parameters]
        (sql-where table-info-registry
                   #(when (= % [])
                      {:table table
                       :alias alias})
                   where)

        sql (str "UPDATE " (q table-name) " AS " alias
                 " SET " (str/join ","
                                   (map #(str (q (first %)) "=?")
                                        cols))
                 " WHERE " where-clause)
        sql-and-params (into [sql] (concat (map second cols)
                                           where-parameters))]
    (first (jdbc/execute! db sql-and-params))))

(s/def ::keyset-record-where
  (s/cat :keyset (s/? (s/and (s/coll-of keyword?)
                             set?))
         :record (s/map-of keyword? any?)
         :where (s/? (s/map-of keyword? any?))))


(defn upsert!
  "Atomically UPDATE or INSERT record"
  [db table & keyset-record-where]
  (assert-table table)
  (let [table-info-registry @table-info-registry
        {table-name :name table-columns :columns
         insert-spec :insert-spec-kw :as table-info}
        (table-info-registry table)

        {:keys [keyset record where]}
        (s/conform ::keyset-record-where keyset-record-where)

        record (assert-spec insert-spec record)

        primary-keys (primary-key-columns table-columns)
        conflict-keys (or keyset
                          (keys primary-keys))
        conflict-target (map (comp :name table-columns)
                             conflict-keys)
        conflict-target-column? (into #{} conflict-target)

        _ (assert (not (empty? conflict-target))
                  (str "No conflict target, if table has no primary key, specify a column set"))
        _ (assert (every? string? conflict-target)
                  (str "Unknown columns in conflict target " (pr-str conflict-keys)))

        ;; If a keyset was provided, make sure all keys are in the input record.
        ;; Dont check for primary keys (they are often autogenerated serials)
        _ (assert (or (nil? keyset)
                      (every? (partial contains? record) conflict-keys))
                  (str "Conflict target contains columns that are not in the record to upsert: "
                       (pr-str (into #{}
                                     (remove (partial contains? record))
                                     conflict-keys))))
        alias-fn (gen-alias)
        alias (alias-fn table)

        [column-names value-names value-parameters]
        (insert-columns-and-values table-info-registry table record)

        [where-clause where-parameters]
        (sql-where table-info-registry
                   #(when (= % [])
                      {:table table
                       :alias alias})
                   where)



        cols (when-not (empty? primary-keys)
               (fetch-columns table-info-registry table alias alias-fn primary-keys))
        sql (str "INSERT INTO \"" table-name "\" AS " alias " "
                 "(" (str/join "," (map q column-names)) ") "
                 "VALUES (" (str/join "," value-names) ") "
                 "ON CONFLICT (" (str/join "," (map q conflict-target)) ") "
                 "DO UPDATE SET " (str/join ","
                                            (keep #(when-not (conflict-target-column? %)
                                                     (str (q %) " = EXCLUDED." (q %)))
                                                  column-names))
                 (when-not (str/blank? where-clause)
                   (str " WHERE "  where-clause))
                 (when cols
                   (str " RETURNING " (sql-columns-list cols))))

        sql-and-params (into [sql]
                             (concat value-parameters where-parameters))]

    ;;(println "SQL: " (pr-str sql-and-params))
    (if (empty? primary-keys)
      ;; No returning clause, execute and check affected rows count
      (if (zero? (first (jdbc/execute! db sql-and-params)))
        nil
        record)

      ;; Returning primary keys, do query and add them to record
      (let [result (first (jdbc/query db sql-and-params))]
        (when result
          (reduce (fn [record [resultset-kw [_ output-kw]]]
                    (assoc-in record output-kw (result resultset-kw)))
                  record
                  cols))))))
