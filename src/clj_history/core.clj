(ns clj-history.core
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn]))

(defn make-archive 
  ([apply-fn zero]
   {:into apply-fn :state zero :history () :zero zero})
  ([apply-fn zero fname]
   (let [f (io/as-file fname)]
     (when (not (.exists f)) 
       (with-open [w (io/writer fname)]
         (.write w (with-out-str (prn zero)))))
     {:into apply-fn :state zero :history {} :zero zero :file f})))

(defn multiplex-archive [arc stream-vector]
  (assoc arc :streams stream-vector))

(defn commit-event [arc event]
  (let [ev-str (with-out-str (prn event))
        file (arc :file)]
    (and file 
         (with-open [w (io/writer file :append :true)]
           (.write w ev-str)))
    (and (arc :streams) (doseq [s (arc :streams)] (.write s ev-str)))))

(defn apply-event [arc event]
  (let [new-arc (assoc arc :state ((arc :into) (arc :state) event))]
    (if (arc :history)
      (assoc new-arc :history (cons event (arc :history)))
      arc)))

(defn new-event [arc event]
  (commit-event arc event)
  (apply-event arc event))

(defn load-archive [fname apply-fn]
  (with-open [in (java.io.PushbackReader. (io/reader fname))]
    (let [arc (make-archive apply-fn (edn/read in) fname)
          eof (gensym)]
      (reduce
       apply-event arc
       (take-while 
        (partial not= eof)
        (repeatedly (partial edn/read {:eof eof} in)))))))

;;;;;;;;;; Testing
(let [ins (fn [arc ev]
            (case (get ev 0)
              :insert (let [[_ k v] ev] (assoc arc k v))
              :delete (let [[_ k] ev] (dissoc arc k))))
      zero {}]
  (defn make-history-table
    ([] (make-archive ins zero))
    ([fname] (make-archive ins zero fname)))
  (defn load-history-table [fname]
    (load-archive fname ins)))
