(ns kotoba.fleet.store
  "Append-only store contract (`:db-api`) + an in-memory MemStore.

  The fleet talks to its backend ONLY through a plain map of pure functions, so
  the same lease/governor/view logic runs on MemStore (here), a real Datomic, or
  the kotoba-db XRPC pod — whichever map is injected. The log is append-only:
  facts are added, never overwritten, so two agents writing concurrently is a
  set-union, not a destructive race.

      {:transact! (fn [datoms] …)   ; append [e a v t] datoms, return {:added n}
       :datoms    (fn [] …)         ; the whole append-only log
       :by        (fn [a v] …)      ; datoms with attribute a and value v
       :entity    (fn [e] …)}       ; pull {:db/id e, a v, …}

  A datom is a vector [e a v t]: entity, attribute (namespaced keyword), value,
  monotonically non-decreasing tx ordinal t (append order = causal order).")

(defn mem-store
  "In-memory append-only MemStore implementing the `:db-api` contract. State is
  an atom holding the datom vector; every transact! only ever conj-es."
  []
  (let [log (atom [])]
    {:transact! (fn [datoms]
                  (let [ds (vec datoms)]
                    (swap! log into ds)
                    {:added (count ds)}))
     :datoms    (fn [] @log)
     :by        (fn [a v]
                  (filterv (fn [[_ da dv]] (and (= da a) (= dv v))) @log))
     :entity    (fn [e]
                  (reduce (fn [m [de a v]] (if (= de e) (assoc m a v) m))
                          {:db/id e}
                          @log))}))

(defn transact!
  "Append `datoms` (a seq of [e a v t]) to the store."
  [db datoms]
  ((:transact! db) datoms))

(defn datoms
  "The whole append-only log."
  [db]
  ((:datoms db)))

(defn by
  "Datoms whose attribute = `a` and value = `v`."
  [db a v]
  ((:by db) a v))

(defn entity
  "Pull the entity map for `e`."
  [db e]
  ((:entity db) e))

(defn next-t
  "Next tx ordinal = current log length. Append order is causal order, which is
  what makes the lease winner deterministic without a lock server."
  [db]
  (count (datoms db)))
