(ns specql.impl.update
  "Implementation of update!"
  (:require [specql.impl.util :refer :all]
            [specql.impl.registry :as registry]
            [specql.impl.where :as where]
            [clojure.string :as str]
            [clojure.java.jdbc :as jdbc]))

(defn update! [db table record where]
  (assert-table table)
  (let [table-info-registry @registry/table-info-registry
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
        (where/sql-where table-info-registry
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
