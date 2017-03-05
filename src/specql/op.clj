(ns specql.op
  "SQL operators"
  (:refer-clojure :exclude [< > <= >= = not= not or and])
  (:require [clojure.string :as str]))

(defprotocol Op
  (to-sql [this value-accessor]))

(defmacro defoperator [sym sql-template]
  (let [optype (symbol (str "Op" (name sym)))
        optype-cons (symbol (str "->Op" (name sym)))
        val (gensym "val")
        arg (gensym "arg")]
    `(do
       (defrecord ~optype [~arg]
         Op
         (to-sql [_# ~val]
           [(str ~@(for [s sql-template]
                     (case s
                       :val val
                       :arg "?"
                       :args `(str/join "," (repeat (count (first ~arg)) "?"))
                       s)))
            ~(if (some #(clojure.core/= :args %) sql-template)
               `(vec (first ~arg))
               arg)]))
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
(defoperator in [:val " IN (" :args  ")"])

(def null?
  (reify Op
    (to-sql [_ v]
      (str v " IS NULL"))))

(def not-null?
  (reify Op
    (to-sql [_ v]
      (str v " IS NOT NULL"))))

(defrecord Opnot [op]
  Op
  (to-sql [_ value-accessor]
    (let [[sql params] (to-sql op value-accessor)]
      [(str "NOT (" sql ")")
       params])))

(defn not [op]
  (->Opnot op))

(defrecord CombinedOp [combine-with ops]
  Op
  (to-sql [_ value-accessor]
    (loop [sql []
           params []
           [op & ops] ops]
      (if-not op
        [(str "(" (str/join combine-with sql) ")") params]
        (let [[op-sql op-params] (to-sql op value-accessor)]
          (recur (conj sql op-sql)
                 (into params op-params)
                 ops))))))

(defn or [& ops]
  (->CombinedOp " OR " ops))

(defn and [& ops]
  (->CombinedOp " AND " ops))
