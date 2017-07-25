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
         (sort-by-suffix table)

         ;; Join table
         (let [[join-field join-table-columns] c
               rel (-> table-info-registry
                       primary-table
                       :rel join-field)
               _ (assert rel
                         (str "Don't know how to fetch joined " join-field ". "
                              "Add missing relations in define-tables call."))
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



(defn- has-many-joins? [table-alias]
  (some #(nth % 5) ;; the many-to-many collection-path
        (rest table-alias)))

(defn- has-many-join-columns
  "If there are many-to-many joins, add postgres physical row identity field to the
  result so that we can use it to detect unique rows.
  The resultset processing will use the ctid fields to detect same rows and
  add nested maps to the correct sequences."
  [table-alias alias-fn]
  (doseq [t table-alias]
    (println "TABLE-ALIAS: " (pr-str t)))
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

(defn- add-physical-row-ids
  "Add the physical table row id for each table we are fetching. This is needed for grouping
  collections."
  [table-alias alias-fn]
  (mapv (fn [[_ table-alias join-type _ _ has-many-collection _ :as a]]
          {:column-alias (keyword (alias-fn :ctid))
           :column-sql (str "CAST(\"" table-alias "\".ctid AS varchar)")
           :collection-path (when (= join-type :left-join)
                              has-many-collection)})
        table-alias))


;; HAS MANY JOINS COULD BE MADE WITH array_agg(ROW(...)) and read in as composites
;; QUERIES
;; (jdbc/query db1 ["SELECT d.id, d.name, (SELECT array_agg(ROW(e.id, e.name)) FROM employee e WHERE e.department=d.id) as emps, (SELECT array_agg(ROW(p.id,p.name)) FROM project p WHERE p.\"department-id\"=d.id) AS projs FROM department d"])


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



#_(def data [(with-meta {:foo "bar"} {::group {:cti1 "11" :cti2 "22"}})
             (with-meta {:foo "baz"} {::group {:cti1 "11" :cti2 "23"}})
             (with-meta {:foo "sky"} {::group {:cti1 "12" :cti2 "24"}})])

(defn- group-nested-collections [key-ctid results]
  "In a sequence of correlated rows (results), find all unique nested items by ctid.
  The key-ctid is a map of keyword in the resulting map and a ctid for unique rows."
  (println "GROUP NESTED: " (pr-str key-ctid)
           (pr-str results))

  ;; DO ONLY IF key count is 1
  ;; in the below fixme recurse to all keys that begin with the current key
  ;;
  (reduce (fn [result-map [ctid key]]
            (if (not= 1 (count key))
              ;; Only do joining for this level (keypath length is one)
              result-map
              (assoc-in result-map
                        key (into #{}
                                  (comp
                                   ;; if key is not nil
                                   (keep (fn [[k v]]
                                           (when k v)))
                                   ;; take the first value in the list
                                   ;; FIXME: group nested collections from those as well
                                   (map (comp #(get-in % key) first)))

                                  (group-by #(get (::group (meta %)) ctid) results)))))
          {}
          key-ctid))

(defn- group-collections-by-physical-row-id [cols results]
  (let [kw (:column-alias (first cols))
        nested-collections (into {}
                                 (comp
                                  (filter #(some? (:collection-path %)))
                                  (map (juxt :column-alias :collection-path)))
                                 cols)
        helper (fn helper [results]
                 (when-not (empty? results)
                   (let [ctid #(get (::group (meta %)) kw)
                         first-ctid (ctid (first results))
                         same-ctid? #(= first-ctid (ctid %))
                         [rows results] (split-with same-ctid? results)]
                     ;; FIXME: group the collections
                     (println "GROUP WITH " first-ctid " HAS " (count rows))
                     (doseq [r rows]
                       (println "ROW: " (pr-str r)))
                     (cons (merge (first rows)
                                  (group-nested-collections nested-collections rows))
                           (helper results)))))]
    (lazy-seq (helper results))))

(defn fetch [db table columns where]
  (assert-table table)
  (assert (and (set? columns)
               (seq columns))
          "Columns must be a non-empty set")
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

        physical-row-id-cols (add-physical-row-ids table-alias alias-fn)
        [group-fn process-collections]
        (if-not (has-many-joins? table-alias)
          ;; No has-many joins, don't do collection processing
          [nil identity]

          ;; Create a function to add rows to collections
          [(let [ctid-keys (into #{} (map :column-alias) physical-row-id-cols)]
             #(select-keys % ctid-keys))
           (fn [results]
             (group-collections-by-physical-row-id physical-row-id-cols results))])

        path->table (path->table-mapping table-alias)
        [where-clause where-parameters]
        (where/sql-where table-info-registry path->table where)

        all-cols (into cols
                       (map (fn [{:keys [column-alias column-sql]}]
                              [column-alias [column-sql]]))
                       physical-row-id-cols)
        sql (str "SELECT " (sql-columns-list all-cols)
                 " FROM " (sql-from table-info-registry table-alias)
                 (when-not (str/blank? where-clause)
                   (str " WHERE " where-clause))

                 ;; Order by physical row ids so that our nested collections
                 ;; stay in order.
                 ;;
                 ;; PENDING: how should user-defined sort order be combined
                 ;; with the need to keep results in ctid order.
                 " ORDER BY " (str/join ", " (map (comp name :column-alias)
                                                  physical-row-id-cols)))
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

    (println "SQL: " (pr-str sql-and-parameters))

    ;; Post process: parse arrays after joined collections
    ;; have been processed. So that we don't unnecessarily parse
    ;; the same array many times
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
                 (let [v (resultset-kw resultset-row)]
                   (if (nil? v)
                     row
                     (let [xf (::xf/transform col)]
                       (assoc-in row output-path
                                 (if xf
                                   (xf/from-sql xf v)
                                   v))))))
               {}
               cols)
              (when group-fn
                {::group (group-fn resultset-row)})))

          ;; Query the generated SQL with the where map arguments
          (jdbc/query db sql-and-parameters)))
        {::sql sql-and-parameters}))))
