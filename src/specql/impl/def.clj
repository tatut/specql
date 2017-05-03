(ns specql.impl.def
  "Containes the define-tables macro and support code."
  (:require [clojure.spec :as s]
            [specql.data-types :as d]
            [specql.impl.catalog :refer :all]
            [specql.impl.registry :as registry :refer :all]
            [specql.impl.composite :as composite]))

(defn- validate-column-types [tables]
  (let [kw-types (->> tables
                      vals
                      (mapcat :columns))]
    (loop [kw-type {}
           [[kw {type :type}] & kw-types] kw-types]
      (when kw
        (let [previous-type (get kw-type kw)]
          (when previous-type
            (assert (= previous-type type)
                    (str "Type mismatch. Keyword " kw
                         " is already defined as \"" previous-type
                         "\" and now trying to define it as \"" type
                         "\". Check that two tables don't have the same column name"
                         " with different column types in the same namespace.")))
          (recur (assoc kw-type kw type)
                 kw-types))))))

(defn- validate-table-names
  "Validate that there are no columns with the same kw as a table."
  [tables]
  (let [all-column-keys (->> tables vals (mapcat (comp keys :columns)) (into #{}))]
    (doseq [[k _] tables]
      (assert (not (all-column-keys k))
              (str "Table/column name clash. Table " k " is also defined as a column.")))))

(defn- validate-table-info
  "Check that there are no name/type clashes in table info."
  [tables]
  (validate-column-types tables)
  (validate-table-names tables))

(defn- cljs?
  "Check if we are compiling cljs"
  []
  (some-> 'cljs.analyzer
          find-ns
          (ns-resolve '*cljs-file*)
          boolean?))

(defmacro define-tables
  "See specql.core/define-tables for documentation."
  [db & tables]
  (let [db (eval db)]
    (let [table-info (into {}
                           (map (fn [[table-name table-keyword opts]]
                                  (let [ns (name (namespace table-keyword))]
                                    [table-keyword
                                     (-> (table-info db table-name)
                                         (assoc :insert-spec-kw
                                                (keyword ns (str (name table-keyword) "-insert")))
                                         (process-columns ns opts)
                                         (assoc :rel opts))])))
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
          table-info (merge @table-info-registry new-table-info)
          cljs? (cljs?)]


      (validate-table-info table-info)
      `(do
         ;; Register table info so that it is available at runtime
         ;; Only for Clojure
         ~(when-not cljs?
            `(swap! table-info-registry merge ~new-table-info))

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

                                      ;; varchar/text field with max length set
                                      (and (#{"text" "varchar"} type)
                                           (pos? (:type-specific-data column))
                                           `(s/and ~(keyword "specql.data-types" type)
                                                   (fn [s#]
                                                     (<= (count s#) ~(- (:type-specific-data column) 4)))))

                                      (keyword "specql.data-types" type)))]
                          :when type-spec]
                      `(s/def ~kw ~(cond
                                     array?
                                     `(s/coll-of ~type-spec)

                                     (not (:not-null? column))
                                     `(s/nilable ~type-spec)

                                     :default
                                     type-spec))))))))))
