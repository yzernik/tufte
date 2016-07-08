(ns taoensso.tufte.impl
  "Private implementation details."
  (:require [clojure.string  :as str]
            [taoensso.encore :as enc :refer-macros ()])
  #+clj (:import [java.util LinkedList]
                 [java.util.concurrent ArrayBlockingQueue])
  #+cljs
  (:require-macros
   [taoensso.tufte.impl :refer
    (new-pdata t-add t-count t-clear new-mutable-times mutable-times?
      nano-time atom?)]))

;;;; Pdata

(defrecord PData [^long __t0]) ; + {<id> <times> :__m-id-stats {<id> <IdStats>}}
(def ^:dynamic *pdata_* "Non-nil iff dynamic profiling active." nil)

;; Would esp. benefit from ^:static support / direct linking / a Java class
(def ^:static pdata-proxy "Non-nil iff thread-local profiling active."
  #+clj
  (let [^ThreadLocal proxy (proxy [ThreadLocal] [])]
    (fn
      ([]        (.get proxy))
      ([new-val] (.set proxy new-val) new-val)))

  #+cljs ; Assuming we have Clojure 1.7+ for Cljs
  (let [state_ (volatile! false)] ; Automatically thread-local in js
    (fn
      ([]                @state_)
      ([new-val] (vreset! state_ new-val)))))

(comment (enc/qb 1e6 (pdata-proxy))) ; 48.39

(defmacro new-pdata [type]
  (case type
    :thread-local  `(PData. (nano-time))
    :dynamic `(atom (PData. (nano-time)))))

;;;; Stats

(defrecord IdStats [^long count ^long time ^long mean ^long mad-sum
                    ^double mad ^long min ^long max])
(defrecord   Stats [__clock]) ; {<id> <IdStats> :__clock <m-clock>}
(defrecord   Clock [?t0 ?t1 ^long total])

;;;; Time tracking
;; We can use mutable time accumulators when thread-local.
;; Note that LinkedList uses more mem but is faster than java.util.ArrayList.

(defmacro t-add [x t]
  `(enc/if-cljs
     (.add ~x ~t)
     (.add ~(with-meta x {:tag 'LinkedList}) ~t)))

(defmacro t-count [x]
  `(enc/if-cljs
     (.size ~x)
     (.size ~(with-meta x {:tag 'LinkedList}))))

(defmacro t-clear [x]
  `(enc/if-cljs
     (.clear ~x)
     (.clear ~(with-meta x {:tag 'LinkedList}))))

(defmacro new-mutable-times  [] `(enc/if-cljs (cljs.core/array-list) (LinkedList.)))
(defmacro     mutable-times? [x]
  `(enc/if-cljs
     (instance? cljs.core/ArrayList  ~x)
     (instance? LinkedList           ~x)))

(defmacro nano-time [] `(enc/if-cljs (enc/nano-time) (System/nanoTime)))
(comment (macroexpand '(nano-time)))

;; Compaction (times->interim-stats) helps to prevent OOMs:
(def      ^:private ^:const nmax-times ">n will trigger compaction" 2000000)
(declare  ^:private times->IdStats)
(defmacro ^:private atom? [x]
  `(enc/if-cljs
     (instance?    cljs.core.Atom ~x)
     (instance? clojure.lang.Atom ~x)))

(defn ^:static capture-time! [pdata-or-pdata_ id t-elapsed]
  (if (atom? pdata-or-pdata_)

    ;; Using immutable thread-safe times, atom for coordination
    (let [pdata_ pdata-or-pdata_
          ?pulled-times
          (loop []
            (let [pdata @pdata_]
              (let [times (get pdata id ())]
                (if (>= (count times) nmax-times)
                  (if (compare-and-set! pdata_ pdata ; Never leave empty
                        (assoc pdata id (conj () t-elapsed)))
                    times ; Pull accumulated times
                    (recur))

                  (if (compare-and-set! pdata_ pdata
                        (assoc pdata id (conj times t-elapsed)))
                    nil
                    (recur))))))]

      (when-let [times ?pulled-times] ; Compact
        (let [id-stats (get-in @pdata_ [:__m-id-stats id])
              id-stats (times->IdStats times id-stats)]
          ;; Can reasonably assume that our id-stats key is uncontended:
          (swap! pdata_ assoc-in [:__m-id-stats id] id-stats))))

    ;; Using mutable thread-local times (cheap), no coordination
    (let [pdata pdata-or-pdata_]
      (if-let [times (get pdata id)]
        (if (>= (long (t-count times)) nmax-times) ; Compact
          (let [m-id-stats (get pdata :__m-id-stats)
                id-stats   (get m-id-stats id)
                id-stats   (times->IdStats times id-stats)
                m-id-stats (assoc m-id-stats id id-stats)]

            (t-clear times)
            (t-add   times t-elapsed) ; Never leave empty
            (pdata-proxy (assoc pdata :__m-id-stats m-id-stats)))

          ;; Common case
          (t-add times t-elapsed))

        ;; Init case
        (let [times (new-mutable-times)]
          (t-add times t-elapsed)
          (pdata-proxy (assoc pdata id times))))))

  nil)

(def ^:private ^:const max-long #+clj Long/MAX_VALUE #+cljs 9007199254740991)
(defn- times->IdStats [times ?interim-id-stats]
  (let [ts-count
        (long
          (if (mutable-times? times)
            (t-count times)
            (count   times)))

        _          (assert (not (zero? ts-count)))
        times      (vec times) ; Faster to reduce
        ts-time    (reduce (fn [^long acc ^long in] (+ acc in)) 0 times)
        ts-mean    (/ (double ts-time) (double ts-count))
        ts-mad-sum (reduce (fn [^long acc ^long in] (+ acc (Math/abs (- in ts-mean)))) 0 times)
        ts-min     (reduce (fn [^long acc ^long in] (if (< in acc) in acc)) max-long     times)
        ts-max     (reduce (fn [^long acc ^long in] (if (> in acc) in acc)) 0            times)]

    (if-let [^IdStats id-stats ?interim-id-stats] ; Merge over previous stats
      (let [s-count   (+ (.-count id-stats) ts-count)
            s-time    (+ (.-time  id-stats) ^long ts-time)
            s-mean    (/ (double s-time) (double s-count))
            s-mad-sum (+ (.mad-sum id-stats) ^long ts-mad-sum)
            s0-min    (.-min id-stats)
            s0-max    (.-max id-stats)]

        ;; Batched "online" MAD calculation here is >= the standard
        ;; Knuth/Welford method, Ref. http://goo.gl/QLSfOc,
        ;;                            http://goo.gl/mx5eSK.

        (IdStats. s-count s-time s-mean s-mad-sum
          (/ (double s-mad-sum) (double s-count))
          (if (< s0-min ^long ts-min) s0-min ts-min)
          (if (> s0-max ^long ts-max) s0-max ts-max)))

      (IdStats. ts-count ts-time ts-mean ts-mad-sum
        (/ (double ts-mad-sum) (double ts-count))
        ts-min
        ts-max))))

(comment (times->IdStats (new-mutable-times) nil))

;;;;

(defn pdata->Stats
  "Nb: recall that we need a *fresh* (pdata-proxy) when doing
  thread-local profiling."
  [^PData current-pdata]
  (let [t1         (nano-time)
        t0         (.__t0  current-pdata)
        m-id-stats (get    current-pdata :__m-id-stats)
        m-times    (dissoc current-pdata :__m-id-stats :__t0)]
    (reduce-kv
      (fn [m id times]
        (assoc m id (times->IdStats times (get m-id-stats id))))
      (Stats. (Clock. t0 t1 (- t1 t0)))
      m-times)))

;;;; Misc

(defn compile-time-pid [id]
  (if (enc/qualified-keyword? id)
    id
    (if (enc/ident? id)
      (keyword (str *ns*) (name id))
      (throw (ex-info "Unexpected `timbre/profiling` id type"
               {:id id :type (type id)})))))

(comment (compile-time-pid :foo))

;; Code shared with Timbre
(def compile-ns-filter "Returns (fn [?ns]) -> truthy."
  (let [compile1
        (fn [x] ; ns-pattern
          (cond
            (enc/re-pattern? x) (fn [ns-str] (re-find x ns-str))
            (string? x)
            (if (enc/str-contains? x "*")
              (let [re
                    (re-pattern
                      (-> (str "^" x "$")
                          (str/replace "." "\\.")
                          (str/replace "*" "(.*)")))]
                (fn [ns-str] (re-find re ns-str)))
              (fn [ns-str] (= ns-str x)))

            :else (throw (ex-info "Unexpected ns-pattern type"
                           {:given x :type (type x)}))))]

    (fn self
      ([ns-pattern] ; Useful for user-level matching
       (let [x ns-pattern]
         (cond
           (map? x) (self (:whitelist x) (:blacklist x))
           (or (vector? x) (set? x)) (self x nil)
           (= x "*") (fn [?ns] true)
           :else
           (let [match? (compile1 x)]
             (fn [?ns] (if (match? (str ?ns)) true))))))

      ([whitelist blacklist]
       (let [white
             (when (seq whitelist)
               (let [match-fns (mapv compile1 whitelist)
                     [m1 & mn] match-fns]
                 (if mn
                   (fn [ns-str] (enc/rsome #(% ns-str) match-fns))
                   (fn [ns-str] (m1 ns-str)))))

             black
             (when (seq blacklist)
               (let [match-fns (mapv compile1 blacklist)
                     [m1 & mn] match-fns]
                 (if mn
                   (fn [ns-str] (not (enc/rsome #(% ns-str) match-fns)))
                   (fn [ns-str] (not (m1 ns-str))))))]
         (cond
           (and white black)
           (fn [?ns]
             (let [ns-str (str ?ns)]
               (if (white ns-str)
                 (if (black ns-str)
                   true))))

           white (fn [?ns] (if (white (str ?ns)) true))
           black (fn [?ns] (if (black (str ?ns)) true))
           :else (fn [?ns] true) ; Common case
           ))))))

(comment
  (def nsf? (compile-ns-filter #{"foo.*" "bar"}))
  (enc/qb 1e5 (nsf? "foo")) ; 20.44
  )

;;;; Output handlers

(enc/defonce handlers_ "{<hid> <handler-fn>}" (atom nil))

#+clj
(enc/defonce ^:private ^ArrayBlockingQueue handler-queue
  "To prevent handlers from tying up execution thread."
  (ArrayBlockingQueue. 1024))

;; Nb we intentionally, silently swallow any handler errors
(defn- handle-blocking! [m]
  (enc/catch-errors* (enc/run-kv! (fn [_ f] (f m)) @handlers_)))

#+clj  (declare ^:private handler-thread_)
#+cljs (defn handle! [m] (handle-blocking! m) nil)
#+clj  (defn handle! [m] (.offer handler-queue m) @handler-thread_ nil)
#+clj
(defonce ^:private handler-thread_
  (delay
    (let [f (fn []
              (loop []
                (let [m (.take handler-queue)]
                  (handle-blocking! m)
                  (recur))))]
      (doto (Thread. f)
        (.setDaemon true)
        (.start)))))
