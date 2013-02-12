(ns datahog.tables
  (:require [clojure.java.jdbc :as sql]))

(def string-table
  {:suffix "string"
   :sql-type "VARCHAR"})

(def integer-table
  {:suffix "integer"
   :sql-type "BIGINT"})

(def boolean-table
  {:suffix "boolean"
   :sql-type "BOOLEAN"})

(def instant-table
  {:suffix "time"
   :sql-type "TIMESTAMP WITH TIME ZONE"})

(def uuid-table
  {:suffix "uuid"
   :sql-type "UUID"})

(def ref-table
  {:suffix "ref"
   :sql-type "UUID"})

(def keyword-table
  {:suffix "keyword"
   :sql-type "VARCHAR"})

(def relation-tables0
  [string-table integer-table boolean-table instant-table uuid-table ref-table keyword-table])

(defn sequences0 [db]
  [(str "CREATE SEQUENCE " (:prefix db) "_tuple_id_seq")])

(defn system-tables0 [db]
  [(str "CREATE TABLE " (:prefix db) "_transactions "
        " (id BIGSERIAL PRIMARY KEY NOT NULL, "
        "  closed BOOLEAN NOT NULL DEFAULT FALSE"
        ")")
   (str "CREATE TABLE " (:prefix db) "_withdrawals"
        " (id BIGINT PRIMARY KEY NOT NULL,"
        "  trn BIGINT REFERENCES " (:prefix db) "_transactions ON DELETE RESTRICT NOT NULL)")])

(defn rel-create0 [db t]
  [(str "CREATE TABLE " (:prefix db) "_" (:suffix t)
        " (id BIGINT             NOT NULL PRIMARY KEY DEFAULT nextval('" (:prefix db) "_tuple_id_seq'), "
        "  ent UUID              NOT NULL,"
        "  rel VARCHAR           NOT NULL,"
        "  obj " (:sql-type t) " NOT NULL,"
        "  trn BIGINT REFERENCES " (:prefix db) "_transactions(id) ON DELETE RESTRICT NOT NULL"
        ")")
   (str "CREATE INDEX " (:prefix db) "_" (:suffix t) "_rel_idx ON " (:prefix db) "_" (:suffix t) " (rel, ent, obj, id, trn)")
   (str "CREATE INDEX " (:prefix db) "_" (:suffix t) "_obj_idx ON " (:prefix db) "_" (:suffix t) " (obj, ent, rel, id, trn)")
   (str "CREATE INDEX " (:prefix db) "_" (:suffix t) "_ent_idx ON " (:prefix db) "_" (:suffix t) " (ent, rel, obj, id, trn)")])

(defn setup0 [db]
  (sql/with-connection (:url db)
    (sql/transaction
     (apply sql/do-commands (sequences0 db))
     (apply sql/do-commands (system-tables0 db))
     (apply sql/do-commands (mapcat #(rel-create0 db %) relation-tables0))))
  nil)