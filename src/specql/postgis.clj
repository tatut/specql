(ns specql.postgis
  "Require this namespace to add composite parsing for geometry."
  (:require [specql.impl.composite :as composite])
  (:import (org.postgis PGgeometry)))

(defmethod composite/parse-value "geometry" [_ string]
  (org.postgis.PGgeometry. string))
