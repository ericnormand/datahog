(ns datahog.transactions
  (:require [clojure.java.jdbc :as sql])
  (:require [datahog.types :as types]))

(def ^{:private true :dynamic true} *trans* nil)
(defn trans []
  *trans*)

(defn get-last-closed-trans [t-table]
  (sql/with-query-results r
    [(str "SELECT MAX(id) FROM " t-table " WHERE closed")]
    (:max (first r))))

(defn read-trans [t-table f]
  (binding [*trans* {:type :read
                     :id (get-last-closed-trans t-table)}]
    (f)))

(def collision-count (atom 0))

(defn create-trans [t-table]
  (or (sql/with-query-results r
        [(str "INSERT INTO " t-table " (id, closed)
               SELECT nextval('ng_transactions_id_seq'::regclass), FALSE
                 WHERE NOT EXISTS (SELECT 1 FROM " t-table " WHERE NOT closed)
               RETURNING id")]
        (:id (first r)))
      (recur t-table)))

(defn write-trans [t-table f]
  (sql/transaction 
   (let [t (create-trans t-table)]
     (binding [*trans* {:type :write
                        :id t}]
       (f))
     (sql/do-prepared (str "UPDATE " t-table " SET closed = TRUE WHERE id = ?") [t]))))

(defn run-trans [db f]
  (if (trans)
    (f)
    (sql/with-connection (:url db)
      (try
        (read-trans (str (:prefix db) "_transactions") f)
        (catch clojure.lang.ExceptionInfo e
          (println "writing . . .")
          (if (= (:reason (ex-data e)) :write)
            (write-trans (str (:prefix db) "_transactions") f)
            (throw e)))))))

(defmacro transaction [db & body]
  `(run-trans ~db (fn [] ~@body)))

