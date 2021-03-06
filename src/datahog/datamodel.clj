(ns datahog.datamodel
  (:require [clojure.java.jdbc :as sql])
  (:require [datahog.types :as types])
  (:require [datahog.transactions :as trans])
  (:require [datahog.utils :as utils]))

(defn assert-tuple [db e r o]
  (let [t (trans/trans)]
    (assert (instance? java.util.UUID e))
    (assert (keyword? r))
    (let [type (get-in db [:types (get-in db [:schema r])])
          table (str (:prefix db) "_" (:suffix (:table type)))]
      (assert type)
      (assert ((:validator type) o))
      (if (= :write (:type t))
        (sql/insert-record table {:ent e :rel (utils/tostr r) :obj o :trn (:id t)})
        (throw (ex-info "Assert." {:reason :write}))))))

(defn withdraw-sql [db t]
  (str "INSERT INTO " (:prefix db) "_withdrawals "
       "(id, trn) SELECT id, ? FROM " t
       " WHERE ent = ? AND rel = ? AND obj = ? "
       "   AND NOT EXISTS (SELECT 1 FROM " (:prefix db) "_withdrawals as x"
       "                     WHERE x.id = id)"))

(defn withdraws-sql [db t]
  (str "INSERT INTO " (:prefix db) "_withdrawals "
       "(id, trn) SELECT id, ? FROM " t
       " WHERE ent = ? AND rel = ?"
       "   AND NOT EXISTS (SELECT 1 FROM " (:prefix db) "_withdrawals as x"
       "                     WHERE x.id = id)"))

(defn withdraw-tuple [db e r o]
  (let [t (trans/trans)]
    (assert (instance? java.util.UUID e))
    (assert (keyword? r))
    (let [type (get-in db [:types (get-in db [:schema r])])
          table (str (:prefix db) "_" (:suffix (:table type)))
          sql (withdraw-sql db table)]
      (assert type)
      (assert ((:validator type) o))
      (if (= :write (:type t))
        (sql/do-prepared sql [(:id t) e (utils/tostr r) o])
        (throw (ex-info "Withdraw." {:reason :write}))))))

(defn withdraw-tuples [db e r]
  (let [t (trans/trans)]
    (assert (instance? java.util.UUID e))
    (assert (keyword? r))
    (let [type (get-in db [:types (get-in db [:schema r])])
          table (str (:prefix db) "_" (:suffix (:table type)))
          sql (withdraws-sql db table)]
      (assert type)
      (if (= :write (:type t))
        (sql/do-prepared sql [(:id t) e (utils/tostr r)])
        (throw (ex-info "Withdraw." {:reason :write}))))))