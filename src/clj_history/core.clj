(ns clj-history.core
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn]))

;;;;;;;;;; Options
;; - An in-memory base (event sequence stored as part of the archive)
;; - An on-disk base (event sequence stored on disk only)
;; - A hybrid (both in-memory and on-disk storage; useful when you want rewind capability AND persistence)
;; - Potentially additional streams to feed events to
;;
;; So, really, what we want here is optional :history list, :file pathname, :streams stream vector.

(defn new-archive [apply-fn zero]
  {:into apply-fn :state zero :history () :zero zero})

(defn stream-write-event [writer ev]
  (.write writer (with-out-str (prn ev))))

(defn file-write-event [fname ev]
  (with-open [w (io/writer fname :append :true)]
    (stream-write-event w ev)))

(defn apply-event [arc event]
  (let [new-arc (assoc arc :state ((arc :into) (arc :state) event))]
    (and (arc :file) (file-write-event (arc :file) event))
    (and (arc :streams) (mapv (fn [s] (stream-write-event s event)) (arc :treams)))
    (if (arc :history)
      (assoc new-arc :history (cons event (arc :history)))
      arc)))

(defn load-archive [fname apply-fn]
  (with-open [in (java.io.PushbackReader. (io/reader fname))]
    (let [arc (new-archive apply-fn (edn/read in))
          eof (gensym)]
      (reduce
       apply-event arc
       (take-while 
        (partial not= eof)
        (repeatedly (partial edn/read {:eof eof} in)))))))

;;;;;;;;;; Testing
(defn history-table []
  (new-archive 
   (fn [arc ev]
     (case (get ev 0)
       :insert (let [[_ k v] ev] (assoc arc k v))
       :delete (let [[_ k] ev] (dissoc arc k))))
   {}))
