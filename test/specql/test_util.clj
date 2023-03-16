(ns specql.test-util
  (:require [clojure.test :refer [is]]))

(defmacro asserted [msg-regex & body]
  `(try
     ;; Newer Clojure throws clojure.lang.Compiler$CompilerException
     (do ~@body
         (is false "Expected expception, but none was thrown."))
     (catch Throwable t#
       (let [msg# (if (instance? clojure.lang.Compiler$CompilerException t#)
                    (ex-message (ex-cause t#))
                    (ex-message t#))]
         (is (re-find ~msg-regex msg#) ~(str "Exception matches: " msg-regex))))))
