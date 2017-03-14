(ns specql.impl.composite
  "Read/write PostgreSQL composite types: arrays and user defined types."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [specql.data-types :as d]
            [specql.impl.registry :as registry])
  (:import (org.postgresql.util PGtokenizer)))

(defn- matching [string start-ch end-ch start-idx]
  (assert (= start-ch (.charAt string start-idx)))
  (loop [i (inc start-idx)
         nesting 0]
    (let [ch (.charAt string i)]
      (cond
        (and (= ch end-ch) (zero? nesting))
        [(subs string (inc start-idx) i) (inc i)]

        (= ch start-ch)
        (recur (inc i) (inc nesting))

        (= ch end-ch)
        (recur (inc i) (dec nesting))

        :default
        (recur (inc i) nesting)))))

(defn- quoted [elements start-idx]
  (assert (= \" (.charAt elements start-idx)))
  (loop [prev-ch nil
         idx (inc start-idx)]
    (let [ch (.charAt elements idx)]
      (if (and (= ch \") (not= prev-ch \\))
        [(-> (subs elements (inc start-idx) idx)
             (str/replace "\\\"" "\"")
             (str/replace "\"" "")) (inc idx)]
        (recur ch (inc idx))))))

(defn until [elements start-idx end-ch]
  (loop [idx (inc start-idx)]
    (if (or (= idx (.length elements))
            (end-ch (.charAt elements idx)))
      [(subs elements start-idx idx) (inc idx)]
      (recur (inc idx)))))

(defn- split-elements [elements idx]
  (let [end (.length elements)]
    (loop [acc []
           idx 0]
      (if (>= idx end)
        acc
        (let [ch (.charAt elements idx)]
          (cond
            ;; Read quoted value
            (= \" ch)
            (let [[elt new-idx] (quoted elements idx)]
              (recur (conj acc elt)
                     (inc new-idx)))

            (= \( ch)
            (let [[elt new-idx] (matching elements \( \) idx)]
              (recur (conj acc (str "(" elt ")"))
                     (inc new-idx)))

            ;; Read non-quoted value
            :default
            (let [[elt new-idx] (until elements idx #(= % \,))]
              (recur (conj acc elt)
                     new-idx))))))))

(declare parse)

(defn- parse-item [table-info-registry type string]
  {:string string :as type})

(defn- parse-composite [table-info-registry {cols :columns :as type} string]
  (let [fields (split-elements (first (matching string \( \) 0)) 0)]
    (into {}
          (map (fn [[key {n :number :as col}]]
                 [key
                  (parse table-info-registry col
                         (get fields (dec n)))] ;; FIXME: parse string to type
                 ))
          cols)))

(defmulti parse-value (fn [t str] t))
(defmethod parse-value "int4" [_ string]
  (Long/parseLong string))
(defmethod parse-value "int8" [_ string]
  (Long/parseLong string))
(defmethod parse-value "varchar" [_ string]
  string)
(defmethod parse-value "numeric" [_ string]
  (bigdec string))
(defmethod parse-value "text" [_ string] string)
(defmethod parse-value "uuid" [ _ string] (java.util.UUID/fromString string))

;; FIXME: support all types here as well


(defn parse [table-info-registry type string]
  (if (= "A" (:category type))
    (let [elements (split-elements (first (matching string \{ \} 0)) 0)
          element-parser
          (if-let [composite-type (table-info-registry (:element-type type))]
            ;; Parse a composite value
            (partial parse-composite table-info-registry composite-type)

            (partial parse-value (:element-type type)))]
      (into []
            (map element-parser)
            elements))

    (if-let [ct (registry/composite-type table-info-registry (:type type))]
      (parse-composite table-info-registry (table-info-registry ct) string)
      (parse-value (:type type) string))))
