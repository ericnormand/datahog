(ns datahog.utils)

(defn tostr [k]
  (if (namespace k)
    (str (namespace k) \/ (name k))
    (name k)))
