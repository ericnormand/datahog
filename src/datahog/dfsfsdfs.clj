(in-ns 'user)


(defn collapse-normand [l p v]
  (cond
   (nil? (seq l))
   nil
   (p (first l))
   (lazy-seq (cons v (collapse-normand (drop-while p l) p v)))
   :otherwise
   (lazy-seq (cons (first l) (collapse-normand (rest l) p v)))))

(defn collapse-normand2
  ([l p v]
     (lazy-seq (collapse-normand2 (seq l) p v nil)))
  ([l p v _]
     (when l
       (lazy-seq
        (let [f (first l)
              r (next l)]
          (if (p f)
            (cons v (collapse-normand2 r p v nil nil))
            (cons f (collapse-normand2 r p v nil)))))))
  ([l p v _ _]
     (when l
       (lazy-seq
        (let [f (first l)
              r (next l)]
          (if (p f)
            (collapse-normand2 r p v nil nil)
            (collapse-normand2 r p v nil)))))))

(defn collapse-normand3
  ([l p v]
     (lazy-seq (collapse-normand2 (seq l) p v nil)))
  ([l p v _]
     (when l
       (lazy-seq
        (let [f (first l)
              r (next l)]
          (if (p f)
            (cons v (collapse-normand2 r p v nil nil))
            (cons f (collapse-normand2 r p v nil)))))))
  ([l p v _ _]
     (when l
       (let [f (first l)
             r (next l)]
         (if (p f)
           (collapse-normand2 r p v nil nil)
           (collapse-normand2 r p v nil))))))

(defn collapse-overthink [xs pred rep]
  (when-let [x (first xs)]
    (lazy-seq 
      (if (pred x)
        (cons rep (collapse-overthink (drop-while pred (rest xs)) pred rep))
        (cons x (collapse-overthink(rest xs) pred rep))))))

(defn collapse-smith [ss pred replacement]  
    (lazy-seq    
          (if-let [s (seq ss)]   
              (let [[f & rr] s]  
               (if (pred f)   
                   (cons replacement (collapse-smith (drop-while pred rr) pred replacement))  
                   (cons f (collapse-smith rr pred replacement)))))))

(defn lazy-collapse
  ([coll is-collapsable-item? collapsed-item-representation] (lazy-collapse coll is-collapsable-item? collapsed-item-representation false))
  ([coll is-collapsable-item? collapsed-item-representation in-collapsable-segment?]
     (let [step (fn [coll in-collapsable-segment?]
                  (when-let [item (first coll)]
                    (if (is-collapsable-item? item)
                      (if in-collapsable-segment?
                        (recur (rest coll) true)
                        (cons collapsed-item-representation (lazy-collapse (rest coll) is-collapsable-item? collapsed-item-representation true)))
                      (cons item (lazy-collapse (rest coll) is-collapsable-item? collapsed-item-representation false)))))]
       (lazy-seq (step coll in-collapsable-segment?)))))

(def test-str "\t    a\r          s\td  \t \r \n         f \r\n")

(defn is-wsp?
  [c]
  (if (#{\space \tab \newline \return} c) true))

(comment
 (map
  (fn [collapse]
    (println (class collapse) (str "|" (apply str (collapse test-str is-wsp? \space)) "|"))
    (time (dotimes [_ 1000000] (collapse test-str is-wsp? \space)))
    (time (dotimes [_ 1000000] (first (collapse test-str is-wsp? \space))))
    (time (dotimes [_ 1000000] (second (collapse test-str is-wsp? \space))))
    (time (dotimes [_ 1000000] (last (collapse test-str is-wsp? \space)))))
  [collapse-normand3 collapse-normand2 lazy-collapse]))