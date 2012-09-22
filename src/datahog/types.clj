(ns datahog.types
  (:require [datahog.tables :as tbl])
  (:require [datahog.utils  :as utils]))

(def string-type
  {:name :string
   :table tbl/string-table
   :validator string?
   :from-db identity
   :to-db identity})

(def integer-type
  {:name :integer
   :table tbl/integer-table
   :validator integer?
   :from-db identity
   :to-db identity})

(def bool-type
  {:name :boolean
   :table tbl/boolean-table
   :validator #(instance? Boolean %)
   :from-db identity
   :to-db identity})

(def instant-type
  {:name :instant
   :table tbl/instant-table
   :validator #(instance? java.util.Date %)
   :from-db identity
   :to-db identity})

(def uuid-type
  {:name :uuid
   :table tbl/uuid-table
   :validator #(instance? java.util.UUID %)
   :from-db identity
   :to-db identity})

(def ref-type
  {:name :ref
   :table tbl/ref-table
   :validator #(instance? java.util.UUID %)
   :from-db identity
   :to-db identity})

(def keyword-type
  {:name :keyword
   :table tbl/string-table
   :validator keyword?
   :from-db keyword
   :to-db utils/tostr})

(def types
  {:string string-type
   :integer integer-type
   :boolean bool-type
   :instant instant-type
   :uuid uuid-type
   :ref ref-type
   :keyword keyword-type})