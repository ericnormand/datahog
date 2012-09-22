(ns datahog.sql
  (:require [clojure.string :as string]))

(defn append
  ([]   "")
  ([s1] s1)
  ([s1 s2]
     (let [[s1 & vs1] (if (string? s1) [s1] s1)
           [s2 & vs2] (if (string? s2) [s2] s2)]
       (into [(str s1 s2)] (concat vs1 vs2))))
  ([s1 s2 & ss]
     (reduce append (append s1 s2) ss)))

(defn join [s ss]
  (cond
   (empty? ss)
   ""
   (empty? (rest ss))
   (first ss)
   :otherwise
   (reduce #(append %1 s %2) ss)))

