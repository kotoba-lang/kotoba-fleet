(ns kotoba.fleet.governor
  "Single-writer proposal drain — the actor invariant for the fleet.

  Agents never write to git directly: they only APPEND `:proposal/*` datoms
  (their write intent). One governor per repo drains the pending proposals, runs
  each through a `gate` (accept/reject), materializes accepted ones via a single
  side-effecting `materialize` hook (the only writer that touches git), and
  appends a `:receipt/*`. Because exactly one governor drains a repo, git never
  sees N concurrent writers — the conflict is gone before git is involved."
  (:require [kotoba.fleet.schema :as schema]
            [kotoba.fleet.store :as store]))

(defn- entities-of
  "Reconstruct every entity of `kind-ns` (e.g. \"proposal\") from the log."
  [db kind-ns]
  (->> (store/datoms db)
       (keep (fn [[e a]] (when (= kind-ns (namespace a)) e)))
       distinct
       (mapv #(store/entity db %))))

(defn submit-proposal!
  "Append a write-intent proposal for `agent` on `work`. Returns the proposal id."
  [db {:keys [work agent payload now]}]
  (let [t   (store/next-t db)
        pid (str "prop|" work "|" agent "|" t)
        ent {:proposal/id pid :proposal/work work :proposal/agent agent
             :proposal/payload payload :proposal/t t}]
    (store/transact! db (schema/entity->datoms (assoc ent :proposal/now now) t))
    pid))

(defn pending-proposals
  "Proposals (in causal order) that have not yet been receipted."
  [db]
  (let [done (->> (entities-of db "receipt")
                  (map :receipt/proposal)
                  (into #{}))]
    (->> (entities-of db "proposal")
         (remove #(contains? done (:proposal/id %)))
         (sort-by :proposal/t))))

(defn- receipt!
  [db proposal verdict reason now]
  (let [t   (store/next-t db)
        rid (str "rcpt|" (:proposal/id proposal) "|" t)
        ent {:receipt/id rid
             :receipt/proposal (:proposal/id proposal)
             :receipt/work (:proposal/work proposal)
             :receipt/verdict verdict
             :receipt/reason reason
             :receipt/t t}]
    (store/transact! db (schema/entity->datoms (assoc ent :receipt/now now) t))
    ent))

(defn drain!
  "Drain pending proposals through `gate` then `materialize` (single writer).
  Opts: {:gate (fn [proposal] truthy?) :materialize (fn [proposal] …) :now t}.
  Accepted proposals are materialized; a throwing materialize is caught and the
  proposal is rejected (never leaves git half-written). Returns the receipts."
  [db {:keys [gate materialize now]}]
  (mapv
   (fn [p]
     (if (gate p)
       (try
         (when materialize (materialize p))
         (receipt! db p :accepted nil now)
         (catch #?(:clj Exception :cljs :default) e
           (receipt! db p :rejected (str "materialize-error: " (ex-message e)) now)))
       (receipt! db p :rejected "gate-rejected" now)))
   (pending-proposals db)))
