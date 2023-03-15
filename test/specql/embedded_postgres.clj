(ns specql.embedded-postgres
  (:import (io.zonky.test.db.postgres.embedded PreparedDbProvider DatabasePreparer))
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]))

(def ^:dynamic db nil)

(defn- run-statements [db resource split]
  (doseq [statement (remove str/blank?
                            (str/split (slurp resource) split))]
    (println "SQL: " statement)
    (jdbc/execute! db statement)))

(defn- create-test-database [db]
  (run-statements db "test/database.sql" #";")
  (run-statements db "test/sprocs.sql" #"-#-"))

(defn- provider []
  (PreparedDbProvider/forPreparer
   (reify DatabasePreparer
     (prepare [this ds]
       (create-test-database {:datasource ds})))))

(defonce db-provider (provider))

(defn reset-db []
  (alter-var-root #'db-provider
                  (fn [_]
                    (provider))))

(defn datasource []
  {:datasource (.createDataSource db-provider)})

(defn with-db [fn]
  (binding [db (datasource)]
    (fn)))
