(ns specql.impl.fetch
  "Fetch implementation"
  (:require [specql.impl.util :refer :all]
            [specql.impl.registry :as registry]
            [specql.impl.composite :as composite]
            [specql.impl.where :as where]
            [specql.rel :as rel]
            [clojure.string :as str]
            [clojure.java.jdbc :as jdbc]
            [specql.transform :as xf]))

(defn- sort-by-suffix
  "A *kludge* to sort tables by suffix. PENDING: rewrite JOIN logic to be smarter.
  Now tables may appear in the wrong order and cause invalid SQL."
  [tables]
  (sort-by (comp #(Integer/valueOf %)
                 second
                 #(re-matches #"\w+(\d+)" %)
                 second)
           tables))

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
     (when-not primary-table-info
       (throw (ex-info "Unknown table, call define-tables!"
                       {:table primary-table})))
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
         (sort-by-suffix table)

         ;; Join table
         (let [[join-field join-table-columns] c
               rel (-> table-info-registry
                       primary-table
                       :rel join-field)
               _ (when-not rel
                   (throw (ex-info (str "Don't know how to fetch joined " join-field ". "
                                        "Add missing relations in define-tables call.")
                                   {:join-field join-field})))
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
                             (into path-prefix [join-field]))
                           primary-table]]
                         nested-joins))
                  cs)))))))

(defn- path->table-mapping [table-alias]
  (into {}
        (mapcat (fn [[table alias _ _ paths]]
                  (for [[_ p] paths]
                    [(subvec p 0 (dec (count p)))
                     {:table table
                      :alias alias}])))
        table-alias))

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

(defn- vectorize-path [row path]
  (when row
    (if (empty? path)
      (cond
        (nil? row) []
        (vector? row) row
        :else [row])
      (do
        (println "update " row " at " (first path))
        (update row (first path)
                (fn [row]
                  (println "VECTORIZE ROW: " row ", PATH: " path)
                  (if (vector? row)
                    (mapv #(vectorize-path % (rest path)) row)
                    (vectorize-path row (rest path)))))))))



;; For ALL joins (not just has-many joins) add the *this table column* field for the left
;; side.
;;
;; Use the ids instead of ctids to do the joins.
;; Use NULL values in those ids to detect if something is missing
;;

(defn- group-collection [results group-kw path]
  (mapv (fn [[group-key group]]
          (let [result (first group)
                items (into [] (keep #(get-in % path) group))]
            (if (not (empty? items))
              (assoc-in result path items)

              ;; Remove key for empty items
              (if (> (count path) 1)
                (update-in result (butlast path) dissoc (last path))
                (dissoc result (first path))))))
        (group-by (comp group-kw ::group meta) results)))

(defn- group-collections [has-many-join-cols results]
  ;; Take all ctids of joins that have the keyword in path (is a prefix)
  ;; grouping function is a select-keys of all relevant ctids at this level
  ;; a deeper nested join will have more ctids

  (reduce (fn [results [result-kw [_ path] :as has-many-join-col]]
            (let [path-parts (into #{} path)

                  ;; Take all ctids of tables that appear in the path
                  ctids (into #{}
                              (keep (fn [[ctid [_ join-path]]]
                                      (when (some path-parts join-path)
                                        ctid)))
                              has-many-join-cols)]

              ;; Group results by selecting the ctid keys
              (group-collection results #(select-keys % ctids) path)))

          results

          ;; Sort by path depth (join deeper collections first)
          (reverse
           (sort-by (fn [[_ [_ path] :as has-many-join-col]]
                      (count path))
                    has-many-join-cols))))


(defn- has-many-join-columns
  "If there are many-to-many joins, add postgres physical row identity field to the
  result so that we can use it to detect unique rows.
  The resultset processing will use the ctid fields to detect same rows and
  add nested maps to the correct sequences."
  [table-alias alias-fn]
  (keep (fn [right]
          (let [left-table-kw (nth right 6)
                left (or (some (fn [[tbl :as table]]
                                 (when (= tbl left-table-kw)
                                   table)) table-alias)
                         (first table-alias))]
            (when-let [has-many-collection (nth right 5)]
              [(keyword (alias-fn :ctid))
               [(str "CAST(\"" (second left) "\".ctid AS varchar)")
                ;; the collection to add to if this id hasn't changed
                has-many-collection]])))
        (rest table-alias)))



(defn- post-process-arrays-fn [path->array-type]
  (let [tir @registry/table-info-registry]
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

(defn- order-by [table-alias
                 columns
                 {order :specql.core/order-by
                  direction :specql.core/order-direction}]
  (when direction
    (when-not (some? order)
      (throw (ex-info "Order direction specified without an order-by column!" {}))))
  (when order
    (let [order-column (get columns order)
          alias (some (fn [[_ alias _ _ columns]]
                        (when (contains? columns order)
                          alias))
                      table-alias)]
      (when-not (some? order-column)
        (throw (ex-info "Unknown order column"
                        {:order-by order})))

      (str " ORDER BY " (when alias
                          (str alias ".")) (q (:name order-column))
           (case direction
             (:asc :ascending nil) " ASC"
             (:desc :descending) " DESC"
             (throw (ex-info "Unrecognized order direction"
                             {:direction direction})))))))

(defn- limit-offset [{limit :specql.core/limit
                      offset :specql.core/offset}]
  (str
   (when limit
     (str " LIMIT " limit))
   (when offset
     (str " OFFSET " offset))))

(defn fetch [db table columns where options]
  (assert-table table)
  (when-not (and (set? columns)
                 (seq columns))
    (throw (ex-info "Columns must be a non-empty set" {})))
  (let [table-info-registry @registry/table-info-registry
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
          [(let [join-keys (into #{} (map first has-many-join-cols))]
             #(select-keys % join-keys))
           (fn [results]
             (group-collections has-many-join-cols results))])

        path->table (path->table-mapping table-alias)
        [where-clause where-parameters]
        (where/sql-where table-info-registry path->table where)

        all-cols (into cols has-many-join-cols)
        sql (str "SELECT " (sql-columns-list all-cols)
                 " FROM " (sql-from table-info-registry table-alias)
                 (when-not (str/blank? where-clause)
                   (str " WHERE " where-clause))
                 (order-by table-alias table-columns options)
                 (limit-offset options))
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

    ;;(println "SQL: " (pr-str sql-and-parameters))

    ;; Post process: parse arrays after joined collections
    ;; have been processed. So that we don't unnecessarily parse
    ;; the same array many times
    (try
      (jdbc/with-db-transaction [db db]
           ;; Use transaction here because we need the connection when reading composite values.
           ;; Don't use with-db-connection as that prevents an outer transaction by closing the
           ;; connection after.
           (with-meta
             (process-collections
              (map
               ;; Process each row and remap the columns
               ;; to the namespaced keys we want.
               (fn [resultset-row]
                 (with-meta
                   (reduce
                    (fn [row [resultset-kw [_ output-path col]]]
                      (let [v (resultset-kw resultset-row)
                            xf (::xf/transform col)
                            from-sql (if xf #(xf/from-sql xf %) identity)]
                        (cond

                          ;; NULL value, don't add it to the result
                          (nil? v) row

                          ;; Composite value, parse it
                          (= "C" (:category col))
                          (assoc-in row output-path
                                    (composite/parse table-info-registry col (.getValue v)))

                          ;; Regular value
                          :default
                          (let [xf (::xf/transform col)]
                            (assoc-in row output-path
                                      (from-sql v))))))
                    {}
                    cols)
                   (when group-fn
                     {::group (group-fn resultset-row)})))

               ;; Query the generated SQL with the where map arguments
               (jdbc/query db sql-and-parameters)))
             {::has-many-join-cols has-many-join-cols
              ::sql sql-and-parameters}))
      (catch Exception e
        (throw (ex-info "Exception thrown in query"
                        {::sql sql-and-parameters
                         :exception e}))))))
