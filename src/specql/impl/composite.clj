(ns specql.impl.composite
  "Read/write PostgreSQL composite types: arrays and user defined types."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [specql.data-types :as d]
            [specql.impl.registry :as registry]
            [specql.transform :as xf])
  (:import (org.postgresql.util PGtokenizer)
           (java.time LocalTime)))

(set! *warn-on-reflection* true)

(declare quoted skip-quoted)

(defn- matching [^String string
                 ^Character start-ch
                 ^Character end-ch
                 start-idx]
  (assert (= start-ch (.charAt string start-idx)))
  (loop [i (inc start-idx)
         nesting 0]
    (let [ch (.charAt string i)]
      (cond

        ;; Encountered a quoted value, skip it completely.
        ;; User given text field values may have start/end characters
        ;; that must not be accounted. eg. "*) this is some text"
        (= ch \")
        (recur (skip-quoted string i) nesting)

        (and (= ch end-ch) (zero? nesting))
        [(subs string (inc start-idx) i) (inc i)]

        (= ch start-ch)
        (recur (inc i) (inc nesting))

        (= ch end-ch)
        (recur (inc i) (dec nesting))

        :default
        (recur (inc i) nesting)))))

(defn- quoted [^String elements start-idx]
  (assert (= \" (.charAt elements start-idx)))
  (let [last-idx (dec (count elements))
        acc (StringBuilder.)]
    (loop [idx (inc start-idx)]
      (let [prev-ch (.charAt elements (dec idx))
            ch (.charAt elements idx)
            next-ch (when (< idx last-idx)
                      (.charAt elements (inc idx)))]
        (cond
          ;; pair of doublequotes: "" -> "
          (= ch next-ch \")
          (do
            (.append acc "\"")
            (recur (+ idx 2)))

          ;; characted quoted with backslash
          (and (= ch \\) next-ch)
          (do
            (.append acc next-ch)
            (recur (+ idx 2)))

          ;; single doublequote, end this quoted value
          (= ch \")
          [(str acc) (inc idx)]

          ;; any other character as is
          :default
          (do
            (.append acc ch)
            (recur (inc idx))))))))

(defn- skip-quoted
  "Like quoted, but doesn't return the quoted string. Returns the index after the quoted string."
  [^String elements start-idx]
  (assert (= \" (.charAt elements start-idx)))
  (let [last-idx (dec (count elements))]
    (loop [idx (inc start-idx)]
      (let [prev-ch (.charAt elements (dec idx))
            ch (.charAt elements idx)
            next-ch (when (< idx last-idx)
                      (.charAt elements (inc idx)))]
        (cond
          ;; pair of doublequotes: "" -> "
          (= ch next-ch \")
          (recur (+ idx 2))

          ;; characted quoted with backslash
          (and (= ch \\) next-ch)
          (recur (+ idx 2))

          ;; single doublequote, end this quoted value
          (= ch \")
          (inc idx)

          ;; any other character as is
          :default
          (recur (inc idx)))))))

(defn until [^String elements ^long start-idx end-ch]
  (loop [idx (inc start-idx)]
    (if (or (= idx (.length elements))
            (end-ch (.charAt elements idx)))
      [(subs elements start-idx idx) (inc idx)]
      (recur (inc idx)))))

(defn- split-elements
  ([^String elements ^long idx] (split-elements elements idx false))
  ([^String elements ^long idx return-nil?]
   (let [end (.length elements)]
     (loop [acc []
            idx 0]
       (if (>= idx end)
         acc
         (let [ch (.charAt elements idx)]
           (cond
             ;; Read quoted value
             (= \" ch)
             (let [[elt ^long new-idx] (quoted elements idx)]
               (recur (conj acc elt)
                      (inc new-idx)))

             (= \( ch)
             (let [[elt ^long new-idx] (matching elements \( \) idx)]
               (recur (conj acc (str "(" elt ")"))
                      (inc new-idx)))

             ;; At "," character, this is an empty value
             (= \, ch)
             (recur (conj acc nil)
                    (inc idx))

             ;; Read non-quoted value
             :default
             (let [[elt ^long new-idx] (until elements idx #(= % \,))]
               (recur (conj acc (if (and return-nil?
                                         (= elt "NULL"))
                                  nil elt))
                      (long new-idx))))))))))

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
                        (xf/from-sql xf val)
                        val)]))))
          (sort-by (comp :number second) cols))))


(defmulti parse-value (fn [t str] t))
(defmethod parse-value "int2" [_ string]
  (Integer/parseInt string))
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

(defmethod parse-value "bool" [_ v]
  (if (str/blank? v)
    nil
    (= "t" v)))

(defn- pg-datetime-format []
  (java.text.SimpleDateFormat. "yyyy-MM-dd HH:mm:ss"))

(defn- pg-date-format []
  (java.text.SimpleDateFormat. "yyyy-MM-dd"))

(defn- pg-datetime [string]
  (.parse ^java.text.SimpleDateFormat (pg-datetime-format)
          string))

(defn- pg-date [string]
  (.parse ^java.text.SimpleDateFormat (pg-date-format) string))

(defmethod parse-value "timestamp" [_ string]
  (pg-datetime string))



(def timestamptz-pattern
  #"^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})\.(\d{6})((\+|\-)\d{1,2})$")

(defmethod parse-value "timestamptz" [_ string]
  (let [[_ year month day hour minute second microsecond tz] (re-matches timestamptz-pattern string)]
    (java.util.Date/from
     (.toInstant
      (java.time.ZonedDateTime/of
       (java.time.LocalDateTime/of (Integer/parseInt year)
                                   (Integer/parseInt month)
                                   (Integer/parseInt day)
                                   (Integer/parseInt hour)
                                   (Integer/parseInt minute)
                                   (Integer/parseInt second)
                                   (* 1000 (Integer/parseInt microsecond)))
       (java.time.ZoneId/of tz))))))

(defmethod parse-value "date" [_ string]
  (pg-date string))

;; FIXME: support all types here as well

(defn parse-enum [values element]
  (assert (values element)
          (str "Unknown enum value: " element ", valid values: " (pr-str values)))
  element)

(defn parse [table-info-registry type string]
  (try
    (if (= "A" (:category type))
      (let [elements (split-elements (first (matching string \{ \} 0)) 0 true)
            element-type (subs (:type type) 1)
            element-parser
            (if-let [composite-or-enum-type (table-info-registry (:element-type type))]
              (let [xf (-> composite-or-enum-type :rel ::xf/transform)
                    from-sql (if xf #(xf/from-sql xf %) identity)]
                (case (:type composite-or-enum-type)
                  :composite
                  ;; Parse a composite value
                  #(when-not (nil? %)
                     (parse-composite table-info-registry composite-or-enum-type %))

                  :enum
                  #(when-not (nil? %)
                     ((comp from-sql (partial parse-enum (:values composite-or-enum-type))) %))))
              #(when-not (nil? %)
                 (parse-value element-type %)))]
        (into []
              (map element-parser)
              elements))

      (if-let [ct (registry/composite-type table-info-registry (:type type))]
        (parse-composite table-info-registry (table-info-registry ct) string)
        (if-let [et (registry/enum-type table-info-registry (:type type))]
          (parse-enum (:values (table-info-registry et)) string)
          (parse-value (:type type) string))))
    (catch Exception e
      (throw (ex-info "Composite parsing failed, this is most likely a bug"
                      {:type type
                       :string string
                       :cause e})))))

;; Write back composite values to postgres string representation

(defmulti stringify-value (fn [t val] (:type t)))

(defmethod stringify-value "timestamp" [_ val]
  (if (nil? val)
    ""
    (.format ^java.text.SimpleDateFormat (pg-datetime-format) val)))

(defmethod stringify-value "date" [_ val]
  (if (nil? val)
    ""
    (.format ^java.text.SimpleDateFormat (pg-date-format) val)))

(defmethod stringify-value "point" [_ vals]
  ;; [x,y] vector to "(x,y)" string
  (str "("
       (str/join "," (map str vals))
       ")"))

(defmethod stringify-value "bool" [_ v]
  (cond
    (nil? v) ""
    (true? v) "t"
    (false? v) "f"))

;; int4,int8,varchar,text,uuid,time,float8,numeric all default to (str val)
(defmethod stringify-value :default [t val]
  (str val))

(def special-characters #{\( \) \{ \} \space \" \' \, \[ \] \\})

(defn pg-quote [string]
  (if (str/blank? string)
    ""
    (if (some special-characters string)
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
