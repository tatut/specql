(ns specql.impl.where
  "SQL WHERE clause generation"
  (:require [specql.impl.util :refer :all]
            [specql.impl.registry :refer [composite-type]]
            [specql.op :as op]
            [clojure.string :as str]
            [specql.transform :as xf]))

(declare sql-where)

(defn- combine-ops [table-info-registry path->table
                    {combine-with :combine-with records :ops :as record}
                    path-prefix]
  (let [records (remove nil? records)]
    (when (seq records)
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
                   records)))))))

(defn- add-where [{:keys [:clause :parameters] :as where}
                  {transform ::xf/transform :keys [enum? type] :as column}
                  column-accessor column-keyword value]
  (if (satisfies? op/Op value)
    ;; This is an operator, call to-sql to create SQL clause and params
    (let [[cl params] (op/to-sql value column-accessor column)]
      (assoc where
             :clause (conj clause cl)
             :parameters (into parameters params)))
    ;; Plain old value, assert that it is valid and create = comparison
    (assoc where
           :clause (conj clause
                         (str column-accessor " = ?"
                              (when enum?
                                (str "::" type))))
           :parameters (conj parameters
                             (transform-value-to-sql column (assert-spec column-keyword value))))))

(defn- return-sql-and-parameters [where-clause]
  (as-> where-clause w
      (update w :clause #(str/join " AND " %))
      ((juxt :clause :parameters) w)))

(defn- recursive-where [table-info-registry path->table where value path-prefix]
  (when (path->table path-prefix)
    (let [[sql params] (sql-where table-info-registry path->table value
                                  path-prefix)]
      (assoc where
             :clause (conj (:clause where) (str "(" sql ")"))
             :parameters (into (:parameters where) params)))))

(defn- composite-columns-where [table-info-registry where alias {col-name :name :as column} value]
  (when-let [composite-columns (and (map? value)
                                    (some->> column :type
                                             (composite-type table-info-registry)
                                             table-info-registry
                                             :columns))]
    ;; composite type: add all fields as separate clauses
    (reduce (fn [where [kw val]]
              (assert (composite-columns kw)
                      (str "Unknown column in where clause: no "
                           kw " in composite type "
                           (composite-type table-info-registry (:type column))))
              (add-where where
                         (composite-columns kw)
                         (str "(" alias ".\"" col-name "\").\""
                              (:name (composite-columns kw)) "\"")
                         kw val))
            where value)))

(defn- where-map [table-info-registry path->table record path-prefix]
  (let [{:keys [table alias]} (path->table path-prefix)
        table-columns (-> table table-info-registry :columns)]
    (return-sql-and-parameters
     (reduce
      (fn [where [column-keyword value]]
        ;; If column is a joined table, it has a mapping in path->table.
        ;; Recursively create clauses for the value
        (if (path->table (into path-prefix [column-keyword]))
          (recursive-where table-info-registry path->table
                           where value (into path-prefix [column-keyword]))

          ;; This is a column in the current table
          (let [{col-name :name :as column} (column-keyword table-columns)]
            (assert column (str "Unknown column in where clause: no " column-keyword " in table " table))
            (or (composite-columns-where table-info-registry where alias column value)
                ;; normal column
                (add-where where (table-columns column-keyword)
                           (str alias ".\"" col-name "\"") column-keyword value)))))
      {:clause [] :parameters []}
      record))))

(defn sql-where
  ([table-info-registry path->table record]
   (sql-where table-info-registry path->table record []))
  ([table-info-registry path->table record path-prefix]
   (if (op/combined-op? record)
     ;; OR/AND on the top level
     (combine-ops table-info-registry path->table record path-prefix)
     ;; Regular map of field to value
     (where-map table-info-registry path->table record path-prefix))))
