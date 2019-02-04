(ns specql.impl.registry
  "The runtime information registry about tables"
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str]))

;; This contains the runtime information about tables
(defonce table-info-registry (atom {}))

(defn type-keyword-by-name
  "Returns a namepaced type keyword for the given database type name.
  The database type can be a user define table or type (registered with define-tables)
  or a database specified type name (like 'text' or 'point').

  If no type is found for the given name, returns nil."
  ([type-name] (type-keyword-by-name @table-info-registry type-name))
  ([table-info-registry type-name]
   (let [type-name (if (str/starts-with? type-name "_")
                     (subs type-name 1)
                     type-name)]
     (or (some (fn [[kw {name :name}]]
                 (when (= name type-name)
                   kw))
               table-info-registry)
         (let [dt (keyword "specql.data-types" type-name)]
           (when (s/get-spec dt)
             dt))))))

(defn type-by-name
  ([type-name] (type-by-name @table-info-registry type-name))
  ([table-info-registry type-name]
   (get table-info-registry
        (type-keyword-by-name table-info-registry type-name))))

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

(defn- remap-columns [columns ns-name column-map transform-column-name]
  (reduce-kv
   (fn [columns name column]
     (assoc columns
            (or
             ;; Take the specified remapped name
             (get column-map name)

             ;; Use provided transformation fn to derive column name
             (when transform-column-name
               (transform-column-name ns-name name))

             ;; If no name or transformation fn given, create a keyword
             ;; in the same namespace as the table
             (keyword ns-name name))
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

(defn- wrap-column-name-check [transform-column-name]
  (when transform-column-name
    (fn [ns name]
      (let [column-name (transform-column-name ns name)]
        (when-not (qualified-keyword? column-name)
          (throw (ex-info "Column names must be ns qualified keywords."
                          {:transform-column-name-result column-name})))
        column-name))))

(defn process-columns [{columns :columns :as table-info} ns-name column-options-map
                       transform-column-name]
  (-> table-info
      (update :columns remap-columns ns-name column-options-map
              (wrap-column-name-check transform-column-name))
      (update :columns transformed column-options-map)))

(defn required-insert? [{:keys [not-null? has-default?]}]
  (and not-null?
       (not has-default?)))

(defn columns [table-kw]
  (when-let [cols (-> @table-info-registry table-kw :columns)]
    (set (keys cols))))

(defn tables []
  (-> @table-info-registry keys set))
