(ns specql.embedded-postgres
  (:import (com.opentable.db.postgres.embedded PreparedDbProvider DatabasePreparer))
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]))

(def ^:dynamic db nil)

(defn- create-test-database [db]
  (doseq [statement (remove str/blank?
                            (str/split (slurp "test/database.sql") #";"))]
    (println "SQL: " statement)
    (jdbc/execute! db statement)))

(defonce db-provider (PreparedDbProvider/forPreparer
                      (reify DatabasePreparer
                        (prepare [this ds]
                          (create-test-database {:datasource ds})))))

(defn datasource []
  {:datasource (.createDataSource db-provider)})

(defn with-db [fn]
  (binding [db (datasource)]
    (fn)))
