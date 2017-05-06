(ns specql.impl.util
  (:require [clojure.spec.alpha :as s]
            [specql.impl.registry :as registry]
            [clojure.string :as str]))

(defn assert-spec
  "Unconditionally assert that value is valid for spec. Returns value."
  [spec value]
  (assert (s/valid? spec value)
          (s/explain-str spec value))
  value)

(defn assert-table [table]
  (assert (@registry/table-info-registry table)
          (str "Unknown table " table ", call define-tables!")))

(defn q
  "Surround string with doublequotes"
  [string]
  (str "\"" string "\""))

(defn sql-columns-list [cols]
  (str/join ", "
            (map (fn [[col-alias [col-name _]]]
                   (str col-name " AS " (name col-alias)))
                 cols)))

(defn gen-alias
  "Returns a function that create successively numbered aliases for keywords."
  []
  (let [alias (volatile! 0)]
    (fn [named]
      (let [n (name named)]
        (str (subs n 0 (min (count n) 3))
             (vswap! alias inc))))))

(defn fetch-columns
  "Return a map of column alias to vector of [sql-accessor result-path]"
  [table-info-registry table table-alias alias-fn column->path]
  (let [table-columns (:columns (table-info-registry table))]
    (reduce
     (fn [cols [column-kw result-path]]
       (let [col (table-columns column-kw)
             name (:name col)
             composite-type-kw (registry/composite-type (:type col))]
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
