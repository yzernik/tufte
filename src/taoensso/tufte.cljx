(ns taoensso.tufte
  "A simple, fast, monitoring profiler for Clojure/Script.

  Quick usage summary:
  ====================
    1. Wrap (and name) interesting forms/fns with `p`/`defnp`.
    2. Use `profiled` to return [<result> ?<thread-local-stats>] vector.
       use `profile`  to return  <result>, and handle[1] ?<thread-local-stats>.
    3. Use `profiled-dynamic`,
           `profile-dynamic` if you want stats for all (dynamic) threads.

    [1] See `set-handler!` to un/register handler fn/s (e.g. for logging or
        further analysis).

    The `profile/d*` macros are used to conditionally[2] *activate profiling*
    of conditional[2] `p` forms within their body: either for the current
    thread, or for the current dynamic binding.

      ;; Activate thread-local profiling while running `my-fn`:
      `(profile 2 :my-profile-id (my-fn))`

      ;; Activate dynamic (multi-threaded) profiling while running `my-fn`:
      `(dynamic-profile 2 :my-profile-id (my-fn))`

    In both cases, any unelided, unfiltered `(p ...)` forms executed by
    `my-fn` will have their call stats recorded.

    [2] Tufte provides compile-time elision, and runtime filtering as follows:
      * `profile/d*` - Elision: [level ns], runtime: [level ns arb-test].
      * `p`          - Elision: [level ns], runtime: [profiling-active?].

    Please see the API docs for more info.

  How/where to use this library:
  ==============================
    Tufte is highly optimized: even without elision, you can usually leave
    profiling code in production (e.g. for sampled profiling, or to detect
    unusual performance behaviour). Tufte's stats maps are well suited to
    programmatic inspection + analysis."

  {:author "Peter Taoussanis (@ptaoussanis)"}
  (:require [taoensso.encore     :as enc]
            [taoensso.tufte.impl :as impl :refer-macros ()])
  (:import  [taoensso.tufte.impl IdStats Stats Clock])
  #+cljs (:require-macros [taoensso.tufte :refer (profiled)]))

;;;; Level filtering

;; We deliberately make it possible to set a min-level > any form level
(defn valid-form-level? [x] (if (#{0 1 2 3 4 5}   x) true false))
(defn valid-min-level?  [x] (if (#{0 1 2 3 4 5 6} x) true false))

(def ^:const invalid-form-level-msg         "Invalid profiling level: should be int e/o #{0 1 2 3 4 5}")
(def ^:const  invalid-min-level-msg "Invalid minimum profiling level: should be int e/o #{0 1 2 3 4 5 6}")

(defn ^:static valid-form-level [x]
  (or (#{0 1 2 3 4 5} x)
      (throw (ex-info invalid-form-level-msg {:given x :type (type x)}))))

(defn ^:static valid-min-level [x]
  (or (#{0 1 2 3 4 5 6} x)
      (throw (ex-info invalid-min-level-msg {:given x :type (type x)}))))

(comment (enc/qb 1e5 (valid-form-level 4))) ; 9.17

(def ^:dynamic  *min-level* "e/o #{0 1 2 3 4 5 6}" 2)
(defn        set-min-level!
  "Sets root binding of minimum profiling level, e/o #{0 1 2 3 4 5 6}."
  [level]
  (valid-min-level level)
  #+cljs (set!             *min-level*         level)
  #+clj  (alter-var-root #'*min-level* (fn [_] level)))

(comment (enc/qb 1e6 *min-level*)) ; 26.5

(defmacro with-min-level
  "Executes body with dynamic minimum profiling level, e/o #{0 1 2 3 4 5 6}."
  [level & body]
  (if (integer? level)
    (do
      (valid-min-level level)
      `(binding [*min-level*                ~level ] ~@body))
    `(binding [*min-level* (valid-min-level ~level)] ~@body)))

;;;; Namespace filtering

(def -compile-ns-filter "Caching `impl/comple-ns-filter`."
  (enc/memoize_ impl/compile-ns-filter))

(def ^:dynamic *ns-filter* "(fn [?ns] -> truthy)." (-compile-ns-filter "*"))

(defn set-ns-pattern!
  "Sets root binding of namespace filter."
  [ns-pattern]
  (let [nsf? (-compile-ns-filter ns-pattern)]
    #+cljs (set!             *ns-filter*        nsf?)
    #+clj  (alter-var-root #'*ns-filter* (fn [_] nsf?))))

(defmacro with-ns-pattern
  "Executes body with dynamic namespace filter."
  [ns-pattern & body]
  `(binding [*ns-filter* (-compile-ns-filter ns-pattern)]
     ~@body))

(comment
  (def nsf? (compile-ns-filter #{"foo.*" "bar"}))
  (nsf? "foo.bar"))

;;;; Combo filtering

#+clj
(def ^:private compile-time-min-level
  (when-let [level (enc/read-sys-val "TUFTE_MIN_LEVEL")]
    (println (str "Compile-time (elision) Tufte min-level: " level))
    (valid-min-level level)))

#+clj
(def ^:private compile-time-ns-filter
  (let [ns-pattern (enc/read-sys-val "TUFTE_NS_PATTERN")]
    (when ns-pattern
      (println (str "Compile-time (elision) Tufte ns-pattern: " ns-pattern)))
    (-compile-ns-filter (or ns-pattern "*"))))

#+clj ; Called only at macro-expansiom time
(defn -elide?
  "Returns true iff level or ns are compile-time filtered."
  [level-form ns-str-form]
  (not
    (and
      (or ; Level okay
        (nil? compile-time-min-level)
        (not (valid-form-level? level-form)) ; Not a compile-time level const
        (>= ^long level-form ^long compile-time-min-level))

      (or ; Namespace okay
        (not (string? ns-str-form)) ; Not a compile-time ns-str const
        (compile-time-ns-filter ns-str-form)))))

(defn may-profile?
  "Returns true iff level and ns are runtime unfiltered."
  ([level   ] (may-profile? level *ns*))
  ([level ns]
   (if (>= ^long (valid-form-level level)
           ^long (do #_valid-min-level *min-level*) ; Assume valid
         )
     (if (*ns-filter* ns) true false))))

(comment (enc/qb 1e5 (may-profile? 2))) ; 13.34

;;;; Output handlers
;; Handlers let us nicely decouple stat creation and consumption.

(defn set-handler!
  "Use to un/register interest in stats output.

  nil  `?handler-fn` => unregister id.
  nnil `?handler-fn` =>   register id:
    `(handler-fn {:level _ :ns-str _ :id _ :stats _ :stats-str _})`
    will be called for stats output produced by any `profile` or
   `profile-dynamic` calls.

  Handler ideas:
    Save to a db, log, `put!` to an appropriate `core.async`
    channel, filter, aggregate, use for a realtime analytics dashboard,
    examine for outliers or unexpected output, ...

  NB: handler errors will be silently swallowed. Please `try`/`catch`
  and appropriately deal with (e.g. log) possible errors *within* your
  handler fns."
  ([handler-id ?handler-fn] (set-handler! handler-id ?handler-fn nil))
  ([handler-id ?handler-fn ?ns-filter]
   (if (nil? ?handler-fn)
     (set (keys (swap! impl/handlers_ dissoc handler-id)))
     (let [f ?handler-fn
           f (if (or (nil? ?ns-filter) (= ?ns-filter "*"))
               f
               (let [nsf? (-compile-ns-filter ?ns-filter)]
                 (fn [m]
                   (when (nsf? (get m :?ns-str))
                     (f m)))))]
       (set (keys (swap! impl/handlers_ assoc  handler-id f)))))))

(defn set-basic-println-handler! []
  (set-handler! :basic-println-handler
    (fn [m] (println (force (:stats-str_ m))))))

(comment (set-basic-println-handler!))

(declare format-stats)
(defmacro -handle! [level id stats]
  `(impl/handle!
     {:level  ~level
      :ns-str ~(str *ns*)
      :id     ~id
      :stats  ~stats
      :stats-str_ (delay (format-stats ~stats))}))

;;;; Some low-level primitives

(defn profiling?
  "Returns e/o #{nil :thread-local :dynamic}."
  []
  (if impl/*pdata_*
    :dynamic
    (if (impl/pdata-proxy)
      :thread-local
      nil)))

(comment (enc/qb 1e6 (profiling?))) ; 48.18

(defn start-thread-local-profiling!
  "Warning: this is a low-level primitive. Prefer higher-level macros
  like `profile` when possible.

  NB: must be accompanied by a call to `stop-thread-local-profiling!`
  (e.g. using `try`/`finally`)."
  []
  (impl/pdata-proxy (impl/new-pdata :thread-local))
  nil)

(defn stop-thread-local-profiling!
  "Warning: this is a low-level primitive."
  []
  (when-let [pdata (impl/pdata-proxy)]
    (let [result (impl/pdata->Stats pdata)]
      (impl/pdata-proxy nil)
      result)))

;;;; Core macros

(defmacro profiled
  "When level+ns+test are unfiltered, executes body with thread-local
  profiling active. Always returns [<body-result> ?<stats>]."
  {:arglists '([level            & body]
               [level :when test & body])}
  [level & sigs]
  (let [[s1 s2 & sn] sigs]
    (if-not (= s1 :when)
      `(profiled ~level :when true ~@sigs)
      (let [test s2, body sn]
        (if (-elide? level (str *ns*))
          `[(do ~@body)]
          `(if (and (may-profile? ~level ~(str *ns*)) ~test)
             (try
               (impl/pdata-proxy (impl/new-pdata :thread-local))
               (let [result# (do ~@body)
                     stats#  (impl/pdata->Stats (impl/pdata-proxy))]
                 [result# stats#])
               (finally (impl/pdata-proxy nil)))
             [(do ~@body)]))))))

(defmacro pspy
  "Profiling spy. When ns is unfiltered and profiling is active, records
  execution time of named body. Always returns body's result."
  {:arglists '([form-id & body] [level form-id & body])}
  [& specs]
  (let [[s1 s2] specs
        [level id body]
        (if (and (integer? s1) (enc/ident? s2))
          [(valid-form-level s1) s2 (nnext specs)]
          [5                     s1  (next specs)] ; Max form level
          )]

    (let [id (impl/compile-time-pid id)]
      (if (-elide? level (str *ns*))
        `(do ~@body)
        ;; Note no runtime `may-profile?` check
        `(let [pdata-or-pdata_# (or impl/*pdata_* (impl/pdata-proxy))]
           (if pdata-or-pdata_#
             (let [t0#     (impl/nano-time)
                   result# (do ~@body)
                   t1#     (impl/nano-time)]
               (impl/capture-time! pdata-or-pdata_# ~id (- t1# t0#))
               result#)
             (do ~@body)))))))

(defmacro p "`pspy` alias" [& specs] `(pspy ~@specs))

(comment
  (macroexpand '(pspy 2 foo/id "foo"))
  (enc/qb 1e5 (profiled 2 (p :p1))) ; 125.36
  (profiled 2 :when (chance 0.5) (p :p1) "foo"))

(defmacro profile
  "When level+ns+test are unfiltered, executes named body with thread-local
  profiling active and dispatches stats to any registered handlers.
  Always returns body's result."
  {:arglists '([level stats-id            & body]
               [level stats-id :when test & body])}
  [level id & sigs]
  (let [id (impl/compile-time-pid id)]
    `(let [[result# stats#] (profiled ~level ~@sigs)]
       (when stats# (-handle! ~level ~id stats#))
       result#)))

(comment (profile 2 :my-id :when true "foo"))

;;;; Public user utils

(defn compile-ns-filter
  "Returns (fn [?ns]) -> truthy. Some example patterns:
    \"foo.bar\", \"foo.bar.*\", #{\"foo\" \"bar\"},
    {:whitelist [\"foo.bar.*\"] :blacklist [\"baz.*\"]}"
  [ns-pattern] (impl/compile-ns-filter ns-pattern))

(defn chance "Returns true with 0<`p`<1 probability."
  [p] (< ^double (rand) (double p)))

;;;; Multi-threaded profiling

(defn merge-stats
  "Merges stats maps from multiple runs or threads.
  Automatically identifies and merges concurrent time windows."
  [s1 s2]
  (if s1
    (if s2
      (let [^Stats s1 s1
            ^Stats s2 s2
            ^Clock clock1 (.-__clock s1)
            ^Clock clock2 (.-__clock s2)
            ^Clock clock3
            (enc/if-lets
              [^long s1-t0 (.-?t0 clock1)
               ^long s1-t1 (.-?t1 clock1)
               ^long s2-t0 (.-?t0 clock2)
               ^long s2-t1 (.-?t1 clock2)
               any-clock-overlap?
               (or (and (<= s2-t0 s1-t1)
                        (>= s2-t1 s1-t0))
                   (and (<= s1-t0 s2-t1)
                        (>= s1-t1 s2-t0)))]

              (let [^long s3-t0 (if (< s1-t0 s2-t0) s1-t0 s2-t0)
                    ^long s3-t1 (if (< s1-t1 s2-t1) s1-t1 s2-t1)]
                (Clock. s3-t0 s3-t1 (- s3-t1 s3-t0)))
              (Clock. nil nil (+ (.-total clock1) (.-total clock2))))

            s1*     (dissoc s1 :__clock)
            s2*     (dissoc s2 :__clock)
            all-ids (into (set (keys s1*)) (keys s2*))]

        (reduce
          (fn [m id]
            (let [sid1 (get s1 id)
                  sid2 (get s2 id)]
              (if sid1
                (if sid2
                  (let [^IdStats sid1 sid1
                        ^IdStats sid2 sid2
                        s1-count   (.-count   sid1)
                        s1-time    (.-time    sid1)
                        s1-mad-sum (.-mad-sum sid1)
                        s1-min     (.-min     sid1)
                        s1-max     (.-max     sid1)

                        s2-count   (.-count   sid2)
                        s2-time    (.-time    sid2)
                        s2-mad-sum (.-mad-sum sid2)
                        s2-min     (.min      sid2)
                        s2-max     (.max      sid2)

                        s3-count   (+ s1-count   s2-count)
                        s3-time    (+ s1-time    s2-time)
                        s3-mad-sum (+ s1-mad-sum s2-mad-sum)]

                    (assoc m id
                      (IdStats.
                        s3-count
                        s3-time
                        (/ (double s3-time) (double s3-count))
                        s3-mad-sum
                        (/ (double s3-mad-sum) (double s3-count))
                        (if (< s1-min s2-min) s1-min s2-min)
                        (if (> s1-max s2-max) s1-max s2-max))))
                  m #_(assoc m id sid1))
                (assoc m id sid2))))
          (assoc s1 :__clock clock3)
          all-ids))
      s1)
    s2))

(defn stats-accumulator
  "Experimental, subject to change!
  Small util to help merge stats maps from multiple runs or threads.
  Returns a stateful fn with arities:
    ([stats-map]) ; Accumulates the given stats (you may call this from any thread)
    ([])          ; Deref: returns the merged value of all accumulated stats"
  []
  (let [acc_ (atom nil)
        reduce-stats_
        (delay
          (let [merge-stats (enc/memoize_ merge-stats)]
            (enc/memoize_ (fn [acc] (reduce merge-stats nil acc)))))]

    (fn stats-accumulator
      ([stats-map] (when stats-map (swap! acc_ conj stats-map)))
      ([] (when-let [acc @acc_] (@reduce-stats_ acc))))))

(defn accumulate-stats "Experimental, subject to change!"
  [stats-accumulator [profiled-result profiled-?stats]]
  (when profiled-?stats (stats-accumulator profiled-?stats))
  profiled-result)

(comment
  (enc/qb 1e5 (stats-accumulator)) ; 5.87
  (let [sacc (stats-accumulator)]
    (accumulate-stats sacc (profiled 2 :foo (p :p1)))
    (accumulate-stats sacc (profiled 2 :bar (p :p2)))
    (sacc)))

(defmacro profiled-dynamic
  "Like `profiled` but executes body with dynamic (multi-threaded) profiling
  active. Always returns [<body-result> ?<stats>]."
  {:arglists '([level            & body]
               [level :when test & body])}
  [level & sigs]
  (let [[s1 s2 & sn] sigs]
    (if-not (= s1 :when)
      `(profiled-dynamic ~level :when true ~@sigs)
      (let [test s2, body sn]
        (if (-elide? level (str *ns*))
          `[(do ~@body)]
          `(if (and (may-profile? ~level ~(str *ns*)) ~test)
             (let [pdata_# (impl/new-pdata :dynamic)]
               (binding [impl/*pdata_* pdata_#]
                 (let [result# (do ~@body)
                       stats#  (impl/pdata->Stats @pdata_#)]
                   [result# stats#])))
             [(do ~@body)]))))))

(defmacro profile-dynamic
  "Like `profile` but executes body with dynamic (multi-threaded) profiling
  active. Always returns body's result."
  {:arglists '([level stats-id            & body]
               [level stats-id :when test & body])}
  [level id & sigs]
  (let [id (impl/compile-time-pid id)]
    `(let [[result# stats#] (profiled-dynamic ~level ~@sigs)]
       (when stats# (-handle! ~level ~id stats#))
       result#)))

;;;; Stats formatting

(defn- perc [n d] (Math/round (/ (double n) (double d) 0.01)))
(comment (perc 14 24))

(defn- ft [nanosecs]
  (let [ns (long nanosecs)] ; Truncate any fractionals
    (cond
      (>= ns 1000000000) (str (enc/round2 (/ ns 1000000000))  "s") ; 1e9
      (>= ns    1000000) (str (enc/round2 (/ ns    1000000)) "ms") ; 1e6
      (>= ns       1000) (str (enc/round2 (/ ns       1000)) "μs") ; 1e3
      :else              (str                ns              "ns"))))

(defn format-stats
  ([stats           ] (format-stats stats :time))
  ([stats sort-field]
   (when stats
     (let [^Stats stats stats
           ^Clock clock (.__clock stats)
           clock-total  (.-total  clock)

           stats*          (dissoc stats  :__clock)
           ^long accounted (reduce-kv (fn [^long acc k v] (+ acc ^long (:time v))) 0 stats*)

           sorted-stat-ids
           (sort-by
             (fn [id] (get-in stats [id sort-field]))
             enc/rcompare
             (keys stats*))

           ^long max-id-width
           (reduce-kv
             (fn [^long acc k v]
               (let [c (count (str k))]
                 (if (> c acc) c acc)))
             #=(count "Accounted Time")
             stats*)]

       #+cljs
       (let [sb
             (reduce
               (fn [acc id]
                 (let [^IdStats id-stats (get stats id)
                       time (.-time id-stats)]
                   (enc/sb-append acc
                     (str
                       {:id      id
                        :n-calls     (.-count id-stats)
                        :min     (ft (.-min   id-stats))
                        :max     (ft (.-max   id-stats))
                        :mad     (ft (.-mad   id-stats))
                        :mean    (ft (.-mean  id-stats))
                        :time%   (perc time clock-total)
                        :time    (ft   time)}
                       "\n"))))
               (enc/str-builder)
               sorted-stat-ids)]

         (enc/sb-append sb "\n")
         (enc/sb-append sb (str "Clock Time: (100%) " (ft clock-total) "\n"))
         (enc/sb-append sb (str "Accounted Time: (" (perc accounted clock-total) "%) " (ft accounted) "\n"))
         (str           sb))

       #+clj
       (let [pattern   (str "%" max-id-width "s %,11d %9s %10s %9s %9s %7d %1s%n")
             s-pattern (str "%" max-id-width  "s %11s %9s %10s %9s %9s %7s %1s%n")
             sb
             (reduce
               (fn [acc id]
                 (let [^IdStats id-stats (get stats id)
                       time (.-time id-stats)]
                   (enc/sb-append acc
                     (format pattern id
                           (.-count id-stats)
                       (ft (.-min   id-stats))
                       (ft (.-max   id-stats))
                       (ft (.-mad   id-stats))
                       (ft (.-mean  id-stats))
                       (perc time clock-total)
                       (ft   time)))))

               (enc/str-builder (format s-pattern "Id" "nCalls" "Min" "Max" "MAD" "Mean" "Time%" "Time"))
               sorted-stat-ids)]

         (enc/sb-append sb (format s-pattern "Clock Time"     "" "" "" "" "" 100 (ft clock-total)))
         (enc/sb-append sb (format s-pattern "Accounted Time" "" "" "" "" "" (perc accounted clock-total) (ft accounted)))
         (str sb))))))

;;;; fnp stuff

(defn -fn-sigs [fn-name sigs]
  (let [single-arity? (vector? (first sigs))
        sigs    (if single-arity? (list sigs) sigs)
        get-id  (if single-arity?
                  (fn [fn-name _params] (keyword (str *ns*) (str "fn_" (name fn-name))))
                  (fn [fn-name  params] (keyword (str *ns*) (str "fn_" (name fn-name) \_ (count params)))))
        new-sigs
        (map
          (fn [[params & others]]
            (let [has-prepost-map?      (and (map? (first others)) (next others))
                  [?prepost-map & body] (if has-prepost-map? others (cons nil others))]
              (if ?prepost-map
                `(~params ~?prepost-map (pspy ~(get-id fn-name params) ~@body))
                `(~params               (pspy ~(get-id fn-name params) ~@body)))))
          sigs)]
    new-sigs))

(defmacro fnp "Like `fn` but wraps fn bodies with `p` macro."
  {:arglists '([name?  [params*] prepost-map? body]
               [name? ([params*] prepost-map? body)+])}
  [& sigs]
  (let [[?fn-name sigs] (if (symbol? (first sigs)) [(first sigs) (next sigs)] [nil sigs])
        new-sigs        (-fn-sigs (or ?fn-name (gensym "")) sigs)]
    (if ?fn-name
      `(fn ~?fn-name ~@new-sigs)
      `(fn           ~@new-sigs))))

(comment
  (-fn-sigs "foo"      '([x]            (* x x)))
  (macroexpand '(fnp     [x]            (* x x)))
  (macroexpand '(fn       [x]            (* x x)))
  (macroexpand '(fnp bob [x] {:pre [x]} (* x x)))
  (macroexpand '(fn       [x] {:pre [x]} (* x x))))

(defmacro defnp "Like `defn` but wraps fn bodies with `p` macro."
  {:arglists
   '([name doc-string? attr-map?  [params*] prepost-map? body]
     [name doc-string? attr-map? ([params*] prepost-map? body)+ attr-map?])}
  [& sigs]
  (let [[fn-name sigs] (enc/name-with-attrs (first sigs) (next sigs))
        new-sigs       (-fn-sigs fn-name sigs)]
    `(defn ~fn-name ~@new-sigs)))

(comment
  (defnp foo "Docstring"                [x]   (* x x))
  (macroexpand '(defnp foo "Docstring"  [x]   (* x x)))
  (macroexpand '(defn  foo "Docstring"  [x]   (* x x)))
  (macroexpand '(defnp foo "Docstring" ([x]   (* x x))
                                       ([x y] (* x y))))
  (profiled 2 :defnp-test (foo 5)))

;;;;

(comment
  (set-basic-println-handler!)
  (defn sleepy-threads []
    (dotimes [n 5]
      (Thread/sleep 100) ; Unaccounted
      (p :future/outer @(future (Thread/sleep 500)))
      @(future (p :future/inner (Thread/sleep 500)))
      (p :1ms    (Thread/sleep 1))
      (p :2s     (Thread/sleep 2000))
      (p :50ms   (Thread/sleep 50))
      (p :rand   (Thread/sleep (if (> 0.5 (rand)) 10 500)))
      (p :10ms   (Thread/sleep 10))
      "Result"))

  (profile         2 :sleepy-threads (sleepy-threads))
  (profile-dynamic 2 :sleepy-threads (sleepy-threads))

  (p :hello "Hello, this is a result") ; Falls through (no data context)

  (defnp arithmetic
    []
    (let [nums (vec (range 1000))]
      (+ (p :fast-sleep (Thread/sleep 1) 10)
         (p :slow-sleep (Thread/sleep 2) 32)
         (p :add  (reduce + nums))
         (p :sub  (reduce - nums))
         (p :mult (reduce * nums))
         (p :div  (reduce / nums)))))

  (profile  2 :arithmetic (dotimes [n 100] (arithmetic)))
  (profile  2 :high-n     (dotimes [n 1e5] (p :p1 nil))) ; 23.11ms
  (profile  2 :high-n     (dotimes [n 1e6] (p :p1 nil))) ; 137.26ms
  (profiled 2 (dotimes [n 1e6] (p :p1 nil)))
  (profiled 2 :when (chance 0.5) "bar"))
