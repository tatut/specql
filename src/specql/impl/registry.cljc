(ns specql.impl.registry
  "The runtime information registry about tables")

;; This contains the runtime information about tables
(defonce table-info-registry (atom {}))

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


(defn array-element-type
  "Add an array element type, if the given column is an array"
  [table-info-registry column]
  (if-not (= "A" (:category column))
    column
    (let [element-type-name (subs (:type column) 1)]
      (assoc column :element-type
             (or (composite-type table-info-registry element-type-name)
                 (enum-type table-info-registry element-type-name)
                 (keyword "specql.data-types" element-type-name))))))

(defn- remap-columns [columns ns-name column-map]
  (reduce-kv
   (fn [columns name column]
     (assoc columns
            (or (get column-map name) (keyword ns-name name))
            column))
   {} columns))

(defn- transformed
  "Add :specql.transform/transform to the column definition"
  [columns column-options-map]
  (reduce-kv
   (fn [columns name column]
     (assoc columns name
            (merge column
                   (select-keys (get column-options-map name)
                                #{:specql.transform/transform}))))
   {} columns))

(defn process-columns [{columns :columns :as table-info} ns-name column-options-map]
  (let [column-options-map (eval column-options-map)]
    (-> table-info
        (update :columns remap-columns ns-name column-options-map)
        (update :columns transformed column-options-map))))

(defn required-insert? [{:keys [not-null? has-default?]}]
  (and not-null?
       (not has-default?)))
