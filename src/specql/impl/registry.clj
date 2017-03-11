(ns specql.impl.registry
  "The runtime information registry about tables")

;; This contains the runtime information about tables
(defonce table-info-registry (atom {}))

(defn process-columns [{columns :columns :as table-info} ns-name]
  (assoc table-info
         :columns (reduce-kv (fn [columns name column]
                               (assoc columns
                                      (keyword ns-name name)
                                      column))
                             {} columns)))

(defn composite-type
  "Find user defined composite type from registry by name."
  ([name] (composite-type @table-info-registry name))
  ([table-info-registry name]
   (some (fn [[key {n :name t :type}]]
           (and (= :composite t)
                (= name n)
                key))
         table-info-registry)))

(defn enum-type
  "Find an enum type from registry by name."
  ([name] (enum-type @table-info-registry name))
  ([table-info-registry name]
   (some (fn [[key {n :name t :type}]]
           (and (= :enum t)
                (= name n)
                key))
         table-info-registry)))

(defn required-insert? [{:keys [not-null? has-default?]}]
  (and not-null?
       (not has-default?)))
