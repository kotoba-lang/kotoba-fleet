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
| **agent loop** | `kotoba.fleet.agent` | the agent side: pull open work → optimistically lease → run the injected `run` fn (a **kotoba-code** session in prod) → submit a `:proposal/*`. Loses the race → backs off. This is the kotoba-code integration seam. |
| **governor-drain** | `kotoba.fleet.governor` | agents only *append* `:proposal/*` datoms; a single per-repo governor gates them and materializes accepted ones to git, appending a `:receipt/*`. → git never sees N concurrent writers (the actor invariant). |
| **fleet-view** | `kotoba.fleet.view` | aggregate live leases / TTL / progress across the fleet into one view. |

The holder decision also has a native, capability-free `.kotoba` profile for
up to four claims on one host-selected work unit. It validates the complete
encoding before selecting the earliest active causal ordinal, rejects duplicate
ordinals and future/negative timestamps, and computes expiry without signed-i64
addition overflow. Datom reconstruction, string/work filtering, atomic ordinal
allocation, append operations, and storage remain host-owned CLJC concerns.

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
`kotoba.fleet.kotoba-store/db-api-store` backs the same contract with a langchain
db-api map (`langchain.db` in-process, or `langchain.kotoba-db` over kotobase.net
XRPC with CACAO) — so the identical lease/governor/agent code runs on a real
Datom log. `clojure -M:kotoba` runs the MemStore ≡ backend parity test.

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

## Running an agent on the fleet (`hosts/nbb`, PoC)

The library's `run` seam is where a coding-agent session plugs in. `hosts/nbb`
injects a **sandboxed session on a murakumo fleet node** instead of a terminal
on the operator's laptop — ADR-2606302000's F3/F4 stages:

```bash
nbb --classpath src:hosts/nbb bin/fleet-sandbox-dispatch.cljs \
    --work examples/work-kotoba-delta.edn --agent a1 --log .fleet/log.edn
```

```
lease (kotoba.fleet.lease)
  └─ run = pinned tarball from the GitHub API → scp → ssh node
           → hosts/nbb/fleet/sandbox_agent.cljs (ReAct loop vs murakumo-main)
       └─ proposal (kotoba.fleet.governor/submit-proposal!)
            └─ gate → materialize (single writer) → receipt
```

| piece | file | role |
|---|---|---|
| file-backed `:db-api` | `hosts/nbb/fleet/filestore.cljs` | the same append-only contract as `MemStore`, but shared by separate OS processes (ordinal assigned under a lock — the file analogue of `swap!`) |
| sandboxed agent | `hosts/nbb/fleet/sandbox_agent.cljs` | runs **on the node**: ephemeral workdir from a pinned tarball, path-confined tools, one allowlisted test command under an OS-level exec backing, enforced budgets, emits a patch |
| admission rules | `hosts/nbb/fleet/gate.cljs` | pure governor rules — lease holder, protected paths, green tests, **exec backing** |
| dispatcher | `bin/fleet-sandbox-dispatch.cljs` | lease → remote run → proposal → gate → materialize → receipt |
| shared log | `hosts/nbb/fleet/kotobase_store.cljs` | the same `:db-api` on kotobase.net, so the log is shared across MACHINES |
| agent identity | `hosts/nbb/fleet/identity.cljs` | the dispatcher's own narrow key (kagi) — CACAO auth + signed receipts |
| observer | `bin/fleet-observe.cljs` | read the fleet log from any machine, with no fleet key |
| host selftest | `hosts/nbb/selftest.cljs` | 8 concurrent OS processes race one lease; gate admission matrix; exec-backing containment; identity + receipt signatures; graph derivation; path confinement |

### Where the log lives

```bash
# one machine
… bin/fleet-sandbox-dispatch.cljs --work w.edn --store file --log .fleet/log.edn
# the fleet (shared across machines)
… bin/fleet-sandbox-dispatch.cljs --work w.edn --store kotobase --db-name fleet-log
# watch it from anywhere, holding no fleet key
nbb --classpath … bin/fleet-observe.cljs --db-name fleet-log --graph <cid> --unit <unit>
```

`--store kotobase` puts the append-only log on kotobase.net's tenant Datom
plane. Auth is a **self-minted CACAO**: the key IS the authority for the graph
its own DID derives, so a dispatcher is granted nothing by anyone and can reach
nothing but its own fleet graph. Writers therefore share one fleet agent key
(as the murakumo CI signer does); an observer needs only the graph CID and any
identity — enough to read the log and compute the same holder, never enough to
forge a claim.

Measured across two machines: the Mac claimed a unit through kotobase.net and
`naphtali`, holding no fleet key, read the same 34-datom log and computed the
same holder.

Two properties of the live edge shaped this and are worth knowing before
extending it: a CACAO is **single-use** (nonce replay is recorded), so every
request mints its own; and the edge's query engine did **not** apply the join
in the four-attribute datom reification (a two-lease log came back as 240
cross-product rows), so a fleet datom is stored as ONE entity with ONE
`pr-str`'d tuple. The remote backend also has no compare-and-swap, so ordinals
can collide; the deterministic `[t id]` tiebreak still converges on one holder
and the dispatcher re-confirms after a settle delay before doing any work.

### Credentials

The dispatcher runs on its own **narrow** identity (`--identity kagi:<item>`,
default `kagi:fleet-agent-sandbox-dispatch`, enrolled in
`manifest/fleet-agents.edn` with grant `fleet-sandbox/*`). It cannot advance a
pin, pass governance quorum, or sign for another agent — a stolen copy can at
most add noise to an append-only log.

Source is fetched from codeload over HTTPS with **no credential** for a public
repo; a private one needs an explicit `FLEET_SOURCE_TOKEN`. Nothing inherits
the operator's ambient `gh` login any more, and which credential class was used
is recorded on the payload.

Every governor decision is signed with that key and the signature lands both in
the log and in `receipts/<work>.edn`, so a receipt is checkable against the
enrolled DID rather than trusted because of where it was found.

### Execution isolation

Confining the file tools is not enough on its own: the agent writes the code
that the test command then runs, so **`:test-cmd` is an execution path the agent
controls**. Everything the agent can cause to execute goes through one function
(`exec!`) and runs under an exec backing — today macOS Seatbelt, which is what
every fleet node actually has (all nodes are macOS 26; no docker/podman/colima,
only `lima` with no instance). The profile denies **all network access**, denies
reads of the node's **real home** (ssh keys, tokens, tailnet state), and permits
writes only inside the ephemeral sandbox.

The backing is proved, not assumed. Each session probes its own containment
before the first model call and **fails closed** if anything gets through, and
the governor rejects a proposal that ran with the backing disabled or whose
probe leaked — even when the tests are green. Opting out is a property of the
work-unit (`:allow-unsandboxed-exec`), never of the runtime.

```bash
nbb hosts/nbb/fleet/sandbox_agent.cljs --exec-probe                  # what does this host contain?
nbb hosts/nbb/fleet/sandbox_agent.cljs --exec-probe --backing none   # negative control
```

Measured on fleet node `naphtali` (macOS 26.2): with the backing, all six
escapes — `~/.ssh` read, home listing, curl egress, node egress, write to home,
write outside the sandbox — are blocked; with `--backing none`, all six succeed.
A containment probe that cannot fail proves nothing, so the negative control is
runnable on the same host.

The backing is a value, so a Linux container / microVM backing can be added
without touching the loop, the tools, or the gate.

What the shape guarantees, independent of how the model behaves: the node gets
an exact pinned commit (never the operator's drifting checkout); the agent
returns a **patch** and holds no credential and no remote, so it cannot commit,
push, or move a pin; the governor re-runs its own checks (lease holder, protected
paths, patch applies to the pinned tree, tests actually green) rather than
trusting the agent's report; and losing the lease race is a backoff, so several
dispatchers can point at one log without a lock server.

Not yet: the exec backing is Seatbelt on a shared node, not a container or
microVM (a compromised process still shares the node's kernel and can see other
processes), and `--materialize dry-run` lands the patch as a local commit for
inspection — nothing is pushed.

## Build

```bash
clojure -M:lint          # clj-kondo (errors fail)
clojure -M:test          # cognitect test-runner — contract tests
nbb --classpath src:hosts/nbb hosts/nbb/selftest.cljs   # nbb host invariants
```

`.cljc` keeps `edn`/`Exception` `#?(:clj …/:cljs …)`-conditional so the core runs
on JVM, ClojureScript, and the kotoba-clj WASM pod alike.

## Status

F0 scaffold (ADR-2606302000 maturity F0–F1): datom schema + the three loops +
contract tests on `MemStore`. Follow-ups: bind to `kotoba-db` XRPC, integrate
`kotoba-code` as the per-agent runtime, and a `murakumo` fleet-view extension.
