# kotoba-fleet

**Fleet-coordination substrate — run many terminal coding agents in parallel
without git conflict.** Zero-dep portable `.cljc`. Design SSoT:
[ADR-2606302000](https://github.com/com-junkawasaki/root/blob/main/90-docs/adr/2606302000-kotoba-fleet-agent-coordination.md).

The thesis: **demote git to a terminal materialize layer and lift coordination
onto an append-only kotoba Datom log.** git conflict is a property of the shared
mutable ref (`main`) + line-based 3-way merge — not of parallelism. kotoba-fleet
makes conflict *structurally impossible* with three primitives:

| primitive | ns | what it gives |
|---|---|---|
| **lease** | `kotoba.fleet.lease` | optimistic, lock-server-free mutual exclusion over work-units. Claim = append a `:lease/*` datom; the holder is the **deterministic earliest active claim** (CRDT-style). TTL + crash re-lease. |
| **governor-drain** | `kotoba.fleet.governor` | agents only *append* `:proposal/*` datoms; a single per-repo governor gates them and materializes accepted ones to git, appending a `:receipt/*`. → git never sees N concurrent writers (the actor invariant). |
| **fleet-view** | `kotoba.fleet.view` | aggregate live leases / TTL / progress across the fleet into one view. |

Everything is **append-only datoms** on an injected `:db-api` (set-union merge,
monotonic, deterministic). The same record runs on the in-memory `MemStore`
here, or on `langchain.db` / `kotoba-db` (kotobase.net XRPC) in production —
contract tests pin `MemStore` behaviour.

## :db-api contract

The store is a plain map of pure functions (inject a real Datomic / kotoba pod
by swapping the map):

```clojure
{:transact! (fn [datoms] …)   ; append [e a v t] datoms (append-only; never overwrites)
 :datoms    (fn [] …)         ; the whole append-only log (vector of [e a v t])
 :by        (fn [a v] …)      ; datoms whose attribute = a and value = v
 :entity    (fn [e] …)}       ; pull entity map {:db/id e a v …}
```

`kotoba.fleet.store/mem-store` is the in-memory implementation used by tests.

## Usage

```clojure
(require '[kotoba.fleet.store :as store]
         '[kotoba.fleet.lease :as lease]
         '[kotoba.fleet.governor :as gov])

(def db (store/mem-store))

;; two agents race for the same work-unit — exactly one becomes holder
(lease/claim! db {:work "orgs/kotoba-lang/foo" :agent "A" :ttl-ms 60000 :now 1000})
(lease/claim! db {:work "orgs/kotoba-lang/foo" :agent "B" :ttl-ms 60000 :now 1001})
(lease/holder db "orgs/kotoba-lang/foo" 1002)   ;=> "A"  (earliest active claim wins; no lock server)

;; agent A proposes a write; the governor gates + materializes (single git writer)
(gov/submit-proposal! db {:work "orgs/kotoba-lang/foo" :agent "A"
                          :payload {:file "x.clj" :patch "…"} :now 1003})
(gov/drain! db {:gate (fn [p] true)                       ; accept/reject
                :materialize (fn [p] (spit-to-git! p))    ; single-writer side effect
                :now 1004})
```

## Build

```bash
clojure -M:lint          # clj-kondo (errors fail)
clojure -M:test          # cognitect test-runner — contract tests
```

`.cljc` keeps `edn`/`Exception` `#?(:clj …/:cljs …)`-conditional so the core runs
on JVM, ClojureScript, and the kotoba-clj WASM pod alike.

## Status

F0 scaffold (ADR-2606302000 maturity F0–F1): datom schema + the three loops +
contract tests on `MemStore`. Follow-ups: bind to `kotoba-db` XRPC, integrate
`kotoba-code` as the per-agent runtime, and a `murakumo` fleet-view extension.
