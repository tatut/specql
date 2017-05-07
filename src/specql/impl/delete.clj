(ns specql.impl.delete
  "Implementation of delete!"
  (:require [specql.impl.util :refer :all]
            [specql.impl.registry :as registry]
            [specql.impl.where :as where]
            [clojure.string :as str]
            [clojure.java.jdbc :as jdbc]))

(defn delete! [db table where]
  (assert-table table)
  (let [table-info-registry @registry/table-info-registry
        {table-name :name} (table-info-registry table)
        alias-fn (gen-alias)
        alias (alias-fn table)
        [where-clause where-parameters]
        (where/sql-where table-info-registry
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
