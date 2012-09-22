(ns datahog.transactions
  (:require [clojure.java.jdbc :as sql])
  (:require [datahog.types :as types]))

(def ^{:private true :dynamic true} *write-trans* nil)
(defn trans []
  *write-trans*)

(defn read-trans [f]
  (sql/transaction 
   (sql/do-commands "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE")
   (f)))

(defn write-trans [t-table f]
  (sql/transaction 
   (sql/do-commands "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
   (sql/do-commands (str "LOCK TABLE " t-table " IN EXCLUSIVE MODE"))
   (let [[t]
         (sql/with-query-results r
           [(str "INSERT INTO " t-table " (id) VALUES (DEFAULT) RETURNING id")]
           (doall r))]
     (binding [*write-trans* (:id t)]
       (f)))))

(defn run-trans [db f]
  (if (trans)
    (f)
    (sql/with-connection (:url db)
      (try
        (read-trans f)
        (catch clojure.lang.ExceptionInfo e
          (if (= (:reason (ex-data e)) :write)
            (write-trans (str (:prefix db) "_transactions") f)
            (throw e)))))))

(defmacro transaction [db & body]
  `(run-trans ~db (fn [] ~@body)))

