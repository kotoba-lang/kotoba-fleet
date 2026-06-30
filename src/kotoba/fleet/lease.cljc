(ns kotoba.fleet.lease
  "Optimistic, lock-server-free mutual exclusion over work-units.

  Claiming a work-unit just APPENDS a `:lease/*` datom — there is no lock server
  and no CAS. The holder is computed deterministically: the **earliest active
  claim** (smallest append ordinal `t`) wins. Two agents racing therefore agree
  on the same winner without coordinating, and the loser simply observes it.
  Leases carry a TTL; an expired (or released) lease drops out, so the next
  earliest active claim — or a fresh one — takes over (crash re-lease)."
  (:require [kotoba.fleet.schema :as schema]
            [kotoba.fleet.store :as store]))

(defn- lease-entities
  "Reconstruct every lease entity (latest attribute values) from the log."
  [db]
  (->> (store/datoms db)
       (keep (fn [[e a]] (when (= "lease" (namespace a)) e)))
       distinct
       (mapv #(store/entity db %))))

(defn expired?
  "True when `now` has reached the lease's granted-at + ttl-ms."
  [lease now]
  (>= now (+ (:lease/granted-at lease) (:lease/ttl-ms lease))))

(defn active-leases
  "Leases on `work` that are neither released nor expired at `now`."
  [db work now]
  (->> (lease-entities db)
       (filter #(= (:lease/work %) work))
       (remove :lease/released)
       (remove #(expired? % now))))

(defn holder
  "The agent currently holding `work` — the earliest active claim (causal order),
  or nil if free. Deterministic across all observers."
  [db work now]
  (->> (active-leases db work now)
       (sort-by (juxt :lease/t :lease/id))
       first
       :lease/agent))

(defn claim!
  "Append a lease claim for `agent` on `work` (TTL `ttl-ms`, granted at `now`).
  Always appends; returns {:ok bool :holder agent :lease-id id}. :ok is true iff
  `agent` is the resulting holder (won the race). No lock server is consulted."
  [db {:keys [work agent ttl-ms now]}]
  (let [t   (store/next-t db)
        lid (str work "|" agent "|" t)
        ent {:lease/id lid :lease/work work :lease/agent agent
             :lease/granted-at now :lease/ttl-ms ttl-ms :lease/t t}]
    (store/transact! db (schema/entity->datoms ent t))
    (let [h (holder db work now)]
      {:ok (= h agent) :holder h :lease-id lid})))

(defn release!
  "Release `agent`'s active lease on `work` (append-only: marks :lease/released).
  Returns {:released lease-id} or nil when the agent held no active lease."
  [db {:keys [work agent now]}]
  (when-let [lid (->> (active-leases db work now)
                      (filter #(= (:lease/agent %) agent))
                      first
                      :lease/id)]
    (let [t (store/next-t db)]
      (store/transact! db [[lid :lease/released true t]])
      {:released lid})))
