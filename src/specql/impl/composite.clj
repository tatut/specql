(ns specql.impl.composite
  "Read/write PostgreSQL composite types: arrays and user defined types."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [specql.data-types :as d]
            [specql.impl.registry :as registry]
            [specql.transform :as xf])
  (:import (org.postgresql.util PGtokenizer)
           (java.time LocalTime)))

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

#_(defn- unquote-one [s]
  (-> s
      (str/replace "\\\"" "\"")
      (str/replace "\"\"" "\\\"")))

#_(defn- quoted [elements start-idx]
  (assert (= \" (.charAt elements start-idx)))
  (let [last-idx (dec (count elements))]
    (loop [prev-ch nil
           idx (inc start-idx)]
      (let [ch (.charAt elements idx)
            next-ch (when (< idx last-idx)
                      (.charAt elements (inc idx)))]
        (if (and (= ch \") (not= prev-ch \\)
                 (not= prev-ch \"))
          [(-> (subs elements (inc start-idx) idx)
               unquote-one) (inc idx)]
          (recur ch (inc idx)))))))

(defn- quoted [elements start-idx]
  (assert (= \" (.charAt elements start-idx)))
  (let [last-idx (dec (count elements))]
    (loop [acc ""
           idx (inc start-idx)]
      (let [prev-ch (.charAt elements (dec idx))
            ch (.charAt elements idx)
            next-ch (when (< idx last-idx)
                      (.charAt elements (inc idx)))]
        (cond
          ;; pair of doublequotes: "" -> "
          (= ch next-ch \")
          (recur (str acc "\"") (+ idx 2))

          ;; characted quoted with backslash
          (and (= ch \\) next-ch)
          (recur (str acc next-ch) (+ idx 2))

          ;; single doublequote, end this quoted value
          (= ch \")
          [acc (inc idx)]

          ;; any other character as is
          :default
          (recur (str acc ch) (inc idx)))))))

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

            (= \{ ch)
            (let [[elt new-idx] (matching elements \{ \} idx)]
              (recur (conj acc (str "{" elt "}"))
                     (inc new-idx)))

            ;; At "," character, this is an empty value
            (= \, ch)
            (recur (conj acc nil)
                   (inc idx))

            ;; Read non-quoted value
            :default
            (let [[elt new-idx] (until elements idx #(= % \,))]
              (recur (conj acc elt)
                     new-idx))))))))

(declare parse)

(defn- parse-item [table-info-registry type string]
  {:string string :as type})

(defn- parse-composite [table-info-registry {cols :columns :as type} string]
  (let [field-values-str (first (matching string \( \) 0))
        fields (split-elements field-values-str 0)]
    (into {}
          (keep-indexed
           (fn [i [key {n :number :as col}]]
             (let [xf (::xf/transform col)
                   val (some->> (get fields i)
                                (parse table-info-registry col))]
               (when val
                 [key (if xf
                        (xf/transform xf val)
                        val)]))))
          (sort-by (comp :number second) cols))))

(defmulti parse-value (fn [t str] t))
(defmethod parse-value "int4" [_ string]
  (Long/parseLong string))
(defmethod parse-value "int8" [_ string]
  (Long/parseLong string))
(defmethod parse-value "varchar" [_ string]
  string)
(defmethod parse-value "bpchar" [_ string]
  string)
(defmethod parse-value "numeric" [_ string]
  (bigdec string))
(defmethod parse-value "text" [_ string] string)
(defmethod parse-value "uuid" [ _ string] (java.util.UUID/fromString string))
(defmethod parse-value "time" [_ string]
  (LocalTime/parse string))
(defmethod parse-value "float8" [_ string]
  (Double/parseDouble string))

(defmethod parse-value "point" [_ string]
  (let [vals (-> string
                 (subs 1 (dec (count string)))
                 (str/split #","))]
    (mapv #(Double/parseDouble %) vals)))

(defn- pg-datetime-format []
  (java.text.SimpleDateFormat. "yyyy-MM-dd HH:mm:ss"))

(defn- pg-date-format []
  (java.text.SimpleDateFormat. "yyyy-MM-dd"))

(defn- pg-datetime [string]
  (.parse (pg-datetime-format)
          string))

(defn- pg-date [string]
  (.parse (pg-date-format) string))

(defmethod parse-value "timestamp" [_ string]
  (pg-datetime string))

(defmethod parse-value "date" [_ string]
  (pg-date string))

;; FIXME: support all types here as well

(defn parse-enum [values element]
  (assert (values element)
          (str "Unknown enum value: " element ", valid values: " (pr-str values)))
  element)

(defn parse [table-info-registry type string]
  ;;(println "PARSE: " (pr-str type) ": " string)
  (if (= "A" (:category type))
    (let [elements (split-elements (first (matching string \{ \} 0)) 0)
          ;;_ (println "ELEMENTS: " elements)
          element-parser
          (if-let [composite-or-enum-type (table-info-registry (:element-type type))]
            (let [xf (-> composite-or-enum-type :rel ::xf/transform)
                  from-sql (if xf #(xf/from-sql xf %) identity)]
              (case (:type composite-or-enum-type)
                :composite
                ;; Parse a composite value
                (partial parse-composite table-info-registry composite-or-enum-type)

                :enum
                (comp from-sql (partial parse-enum (:values composite-or-enum-type)))))

            (partial parse-value (:element-type type)))]
      (into []
            (map element-parser)
            elements))

    (if-let [ct (registry/composite-type table-info-registry (:type type))]
      (parse-composite table-info-registry (table-info-registry ct) string)
      (if-let [et (registry/enum-type table-info-registry (:type type))]
        (parse-enum (:values (table-info-registry et)) string)
        (parse-value (:type type) string)))))

;; Write back composite values to postgres string representation

(defmulti stringify-value (fn [t val] (:type t)))

(defmethod stringify-value "timestamp" [_ val]
  (if (nil? val)
    ""
    (.format (pg-datetime-format) val)))

(defmethod stringify-value "date" [_ val]
  (if (nil? val)
    ""
    (.format (pg-date-format) val)))

(defmethod stringify-value "point" [_ vals]
  ;; [x,y] vector to "(x,y)" string
  (str "("
       (str/join "," (map str vals))
       ")"))

;; int4,int8,varchar,text,uuid,time,float8,numeric all default to (str val)
(defmethod stringify-value :default [t val]
  (str val))

(defn pg-quote [string]
  (if (str/blank? string)
    ""
    (if (or
         (str/starts-with? string "(")
         (str/starts-with? string "{")
         (str/includes? string " ")
         (str/includes? string "\""))
      (str "\""
           (-> string
               (str/replace "\\" "\\\\")
               (str/replace "\"" "\\\""))
           "\"")
      string)))

(declare stringify)

(defn stringify-composite
  [table-info-registry {columns :columns :as type} value]
  (str "("
       (str/join ","
                 (for [[kw col] (sort-by (comp :number second) columns)]
                   (stringify table-info-registry col (get value kw))))
       ")"))

(defn- transform [{category :category :as type} value]
  ;; Transform may be defined in the column or in a table :rel options
  (if-let [xf (or (::xf/transform type)
                  (::xf/transform (:rel type)))]
    (if (= "A" category)
      (mapv (partial xf/to-sql xf) value)
      (xf/to-sql xf value))
    value))

(defn stringify
  ([table-info-registry type value]
   (stringify table-info-registry type value false))
  ([table-info-registry type value top-level?]
   (let [value (transform type value)]
     ((if top-level? identity pg-quote)
      (cond
        (= "A" (:category type))
        (let [element-type-name (:element-type type)
              element-type (if (keyword? element-type-name)
                             (table-info-registry element-type-name)
                             (registry/type-by-name table-info-registry element-type-name))
              stringify-element-type (partial stringify table-info-registry element-type)]
          (str "{" (str/join "," (map stringify-element-type value)) "}"))

        (= :composite (:type type))
        (stringify-composite table-info-registry type value)

        ;; Inner level composite
        (= "C" (:category type))
        (let [composite-type-kw (registry/composite-type table-info-registry (:type type))
              composite-type (table-info-registry composite-type-kw)]
          (stringify-composite table-info-registry
                               composite-type
                               value))

        :default
        (stringify-value type value))))))
