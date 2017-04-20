(ns specql.impl.fetch
  "Fetch implementation"
  (:require [specql.impl.util :refer :all]
            [specql.impl.registry :as registry]
            [specql.impl.composite :as composite]
            [specql.impl.where :as where]
            [specql.rel :as rel]
            [clojure.string :as str]
            [clojure.java.jdbc :as jdbc]))

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
                                         #(cond
                                            (nil? %) []
                                            (vector? %) %
                                            :else [%])))
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



(defn fetch [db table columns where]
  (assert-table table)
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
          (collection-processing-fn has-many-join-cols))

        path->table (path->table-mapping table-alias)
        [where-clause where-parameters]
        (where/sql-where table-info-registry path->table where)

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
