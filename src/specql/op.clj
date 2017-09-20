(ns specql.op
  "SQL operators"
  (:refer-clojure :exclude [< > <= >= = not= not or and])
  (:require [clojure.string :as str]
            [specql.transform :as xf]))

(defprotocol Op
  (to-sql [this value-accessor column-info]
    "Convert the operator to SQL, must return a vector containing
  a SQL fragment and possible parameters. The column-info provides the
  introspected information about the column, like its type and possible
  transformation to apply."))

(defn operator? [x]
  (satisfies? Op x))

(defn- expand-sql-template [sql-template val arg param]
  (for [s sql-template]
    (case s
      :val val
      :arg param
      :args `(str/join "," (repeat (count (first ~arg)) param))
      s)))

(defmacro defoperator [sym sql-template]
  (let [optype (symbol (str "Op" (name sym)))
        optype-cons (symbol (str "->Op" (name sym)))
        val (gensym "val")
        arg (gensym "arg")
        param (gensym "param")]
    `(do
       (defrecord ~optype [~arg]
         Op
         (to-sql [_# ~val column-info#]
           (let [transform# (::xf/transform column-info#)
                 type# (:type column-info#)
                 ~param (if (:enum? column-info#)
                          (str "?::" type#)
                          "?")
                 ~arg (if transform#
                        (map (fn [a#]
                               (xf/to-sql transform# a#)) ~arg)
                        ~arg)]
             [(str ~@(expand-sql-template sql-template val arg param))
              ~(if (some #(clojure.core/= :args %) sql-template)
                 `(vec (first ~arg))
                 arg)])))
       (defn ~sym [& arg#]
         (~optype-cons arg#)))))

(defoperator = [:val " = " :arg])
(defoperator not= [:val " != " :arg])
(defoperator < [:val " < " :arg])
(defoperator <= [:val " <= " :arg])
(defoperator > [:val " > " :arg])
(defoperator >= [:val " >= " :arg])
(defoperator between ["(" :val " BETWEEN " :arg " AND " :arg ")"])
(defoperator like [:val " LIKE " :arg])
(defoperator ilike [:val " ILIKE " :arg])

(defrecord Inop [values]
  Op
  (to-sql [_ v {transform ::xf/transform type :type enum? :enum? :as column-info}]
    (let [values (if transform
                   (map #(xf/to-sql transform %) values)
                   values)
          param (if enum?
                  (str "?::" type)
                  "?")]
      [(str v " IN ("
            (if (empty? values)
              "NULL"
              (str/join "," (repeat (count values) param)))
            ")")
       (vec values)])))

(defn in [values]
  (assert (clojure.core/and
           (clojure.core/or (nil? values)
                            (coll? values))
           (clojure.core/not (map? values)))
          "IN op requires a collection of values")
  (->Inop values))

(def null?
  (reify Op
    (to-sql [_ v c]
      [(str v " IS NULL") []])))

(def not-null?
  (reify Op
    (to-sql [_ v c]
      [(str v " IS NOT NULL") []])))

(defrecord Opnot [op]
  Op
  (to-sql [_ value-accessor column-info]
    (let [[sql params] (to-sql op value-accessor column-info)]
      [(str "NOT (" sql ")")
       params])))

(defn not [op]
  (->Opnot op))

(defrecord CombinedOp [combine-with ops]
  Op
  (to-sql [_ value-accessor column-info]
    (loop [sql []
           params []
           [op & ops] (remove nil? ops)]
      (if-not op
        [(str "(" (str/join combine-with sql) ")") params]
        (let [[op-sql op-params] (to-sql op value-accessor column-info)]
          (recur (conj sql op-sql)
                 (into params op-params)
                 ops))))))

(defn combined-op? [x]
  (instance? CombinedOp x))

(defn or [& ops]
  (->CombinedOp " OR " ops))

(defn and [& ops]
  (->CombinedOp " AND " ops))
