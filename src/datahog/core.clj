(ns datahog.core
  (:require [clojure.set          :as set])
  (:require [clojure.string       :as string])
  (:require [datahog.types        :as types])
  (:require [clojure.java.jdbc    :as sql])
  (:require [datahog.transactions :as trans])
  (:require [datahog.datamodel    :as write])
  (:require [datahog.sql          :as q])
  (:require [datahog.utils        :as utils]))

(defn variable? [c]
  (and (symbol? c) (.startsWith (utils/tostr c) "?")))

(defn vars [coll]
  (set (filter variable? coll)))

(defn bind? [bindings var val]
  (when bindings
   (if (contains? bindings var)
     (if (= (bindings var) val)
       bindings
       nil)
     (assoc bindings var val))))

(defn bind-vars [bindings vars types]
  (let [z (map #(vector %1 %2) vars types)
        vt (filter #(variable? (first %)) z)]
    (reduce #(apply bind? %1 %2) vt)))

(defn isoftype [type val]
  ({:string string?
    :int    integer?
    :ref    integer?} type) val)



(defn type-inference [bindings reltypes stmts]
  (when bindings
    (if (empty? stmts)
      [bindings reltypes]
      (let [[a r v] (first stmts)
            b (cond (variable? a)
                    (or
                     (bind? bindings a :ref)
                     (println "First value must be a ref (integer), found:" a))
                    (integer? a)
                    bindings
                    :otherwise
                    (println "First value must be a ref (integer), found:" a))
            b (cond (variable? r)
                    (bind? b r :string)
                    (string? r)
                    b
                    (keyword? r)
                    b
                    :otherwise
                    (println "Second value must be a string, found:" r))
            t (reltypes (keyword r))
            b (cond (and t (variable? v))
                    (bind? b v t)
                    
                    (variable? v)
                    b
                    
                    (and t (isoftype t v))
                    b
                    
                    :otherwise
                    (println "Inconsistent type in third value, found:" v))
            rs (if t (assoc reltypes r t) reltypes)]
        (recur b rs (rest stmts))))))

(defn typeof [b v]
  (cond
   (variable? v)
   (b v :unknown)
   (string? v)
   :string
   (integer? v)
   :integer
   :otherwise
   :unknown))

(defn merge-locations [locations k table col]
  (if (variable? k)
    (if (locations k)
      [locations (locations k)]
      [(assoc locations k {:table table :col col}) nil])
    [locations k]))

(defn generate-lines
  ([bindings reltypes lines]
     (generate-lines bindings reltypes lines {} 0))
  ([bindings reltypes lines locations p]
   (if (seq lines)
     (let [[a r v] (first lines)
           name (str "line" p)
           type (typeof bindings v)
           [locations aa] (merge-locations locations a name "ent")
           [locations rr] (merge-locations locations r name "rel")
           [locations vv] (merge-locations locations v name "obj")

           line {:type type :name name}
           line (if aa (assoc line :ent aa) line)
           line (if rr (assoc line :rel rr) line)
           line (if vv (assoc line :obj vv) line)
           [locations lines] (generate-lines bindings reltypes
                                             (next lines) locations (inc p))]
       [locations (cons line lines)])
     [locations ()])))

(defn enhance-query [reltypes out where]
  (let [types (type-inference {} reltypes where)]
    (if types
      (let [[bindings reltypes] types
            [locations lines] (generate-lines bindings reltypes where)
            select (vec (for [o out]
                          (if (variable? o)
                            {:name (.substring (name o) 1)
                             :sql (locations o)}
                            o)))]
        {:select select
         :lines lines}))))

(defn valtosql [o]
  (if (map? o)
    (str (:table (:sql o)) "." (:col (:sql o)) " as " (:name o))
    "?"))

(defn where-col [coll types c line]
  (let [to-type {:rel :keyword :ent :ref :obj (:type line)}
        e ((keyword c) line)]
    (if e
      (conj coll
            (if (map? e)
              (str (:name line)  "." (name c) " = "
                   (:table e)    "." (:col e))
              [(str (:name line) "." (name c) " = ?")
               ((:to-db (types (to-type (keyword c)))) e)]))
      coll)))

(defn toselect [out]
  (into [(str "SELECT " (string/join ", " (map valtosql out)))]
        (remove map? out)))

(defn tofrom [db types line]
  (str "FROM " (:prefix db) "_" (:suffix (:table (types (:type line)))) " as " (:name line)))

(defn tojoin [db types line]
  (q/append
   (str "JOIN " (:prefix db) "_" (:suffix (:table (types (:type line))))
        " as " (:name line) " ON (")
   (q/join
    " AND "
    (-> []
        (where-col types :ent line)
        (where-col types :rel line)
        (where-col types :obj line)
        (conj (str "NOT EXISTS (SELECT 1 FROM " (str (:prefix db) "_withdrawals") " AS wd WHERE wd.id = "
                   (:name line) ".id)"))))
   ")"))

(defn towhere [wd types line]
  (append
   "WHERE "
   (q/join
    " AND "
    (-> []
        (where-col types :ent line)
        (where-col types :rel line)
        (where-col types :obj line)
        (conj (str "NOT EXISTS (SELECT 1 FROM " wd " AS wd WHERE wd.id = "
                   (:name line) ".id)"))))))

(defn tosql [db types query]
  (let [wd (str (:prefix db) "_withdrawals")
        from-line (first (:lines query))
        other-lines (rest (:lines query))]
    (q/join " "
            [(toselect (:select query))
             (tofrom db types from-line)
             (q/join " " (map #(tojoin db types %) other-lines))
             (towhere wd types from-line)])))

(defn runquery [db types schemas [out lines]]
  (let [q (tosql db types (enhance-query schemas out lines))]
    (println q)
    (sql/with-connection (:url db)
      (sql/with-query-results res [q]
        (doall res)))))

(defn view-query [db types schemas [out lines]]
  (tosql db types (enhance-query schemas out lines)))