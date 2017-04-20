(ns specql.impl.jdbc
  "JDBC extensions"
  (:require [clojure.java.jdbc :as jdbc]))

;; Extend java.util.Date to be a SQL timestamp parameter
(extend-protocol jdbc/ISQLValue
  java.util.Date
  (sql-value [dt]
    (java.sql.Timestamp. (.getTime dt))))
