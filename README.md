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
| **agent loop** | `kotoba.fleet.agent` | the agent side: pull open work → optimistically lease → run the injected `run` fn (in prod: the **nbb sandbox agent** on a fleet node) → submit a `:proposal/*`. Loses the race → backs off. This is the coding-agent integration seam. |
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

### Kotoba Capability Machine (KCM)

The preferred execution profile is now the **Kotoba Capability Machine**. It
is not a general-purpose VM with a CLI allowlist. A KCM is identified by the
hash of one closed contract:

```
definition closure CID + compiler CID + module-lock CID + target ABI
  + HostCaps policy CID + provider closure SHA-256 + Node runtime SHA-256
  + declared Kotoba checks = KCM machine id
```

The model receives only typed code operations and `kotoba_check <id>`. It does
not receive a shell, process-spawn tool, executable name, or argv. The host
looks up the check id in the KCM contract and appends its fixed arguments to a
verified provider entrypoint, without a shell. The provider bundle includes the
compiler sources, the exact git-locked dependency closure, NBB, and its npm
dependencies. A sorted manifest hashes every regular file; symlinks, extra
files, missing files, a changed archive, or different Node runtime bytes fail
closed before execution (including on a cache hit). PATH is never consulted,
so another executable named `kotoba` cannot become the compiler. The governor
recomputes the machine id and rejects a receipt from any other closure/policy.

```edn
{:machine :kotoba-capability-machine
 :kcm/identity
 {:definition-closure-cid "bafy..."
  :compiler-cid "bafy..."
  :module-lock-cid "bafy..."
  :target-abi :wasm32-component-v1
 :hostcaps-policy-cid "bafy..."
  :provider-closure-sha256 "0123..."
  :runtime-sha256 "4567..."
  :effects []}
 :kcm/provider
 {:archive "/tmp/kotoba-provider.tar"
  :manifest "/tmp/kotoba-provider.manifest.edn"
  :archive-sha256 "89ab..."}
 :kcm/capabilities #{:code/list :code/read :code/edit :build/check :build/compile}
 :kcm/checks
 [{:id :test
   :args ["check" "src/main.kotoba"]
   :pure? true}]
 :kcm/builds
 [{:id :wasm
   :args ["compile" "src/main.kotoba" "--target" "wasm32-wasi"
          "--output" "target/main.wasm"]}]}
```

Build and verify the provider without installing a system-wide CLI:

```sh
npm ci --ignore-scripts --omit=optional --prefix ../compiler
nbb --classpath hosts/nbb scripts/build-kcm-provider.cljs \
  --compiler ../compiler --out /tmp/kotoba-provider
nbb scripts/kcm-coldstart-probe.cljs
nbb scripts/kcm-node-probe.cljs --node naphtali
```

For a first customer evaluation, the repository, compiler checkout, and an
explicit policy are enough. The evaluator hashes the complete source closure,
builds and verifies the pinned compiler provider, proves host containment,
runs every declared check twice (cold miss or prior hit, then a verified hit),
runs declared builds, and emits one content-addressed EDN report:

```sh
nbb --classpath hosts/nbb bin/kcm-evaluate.cljs \
  --repo examples/kcm-evaluate \
  --policy examples/kcm-evaluate/policy.edn \
  --compiler ../compiler \
  --out build/kcm-evaluation.edn
```

The report is `:accepted` only when all checks and builds exit zero and all six
containment probes are blocked. Policies can select only the closed KCM
capability vocabulary; absolute paths, home paths, parent traversal, shell or
process capabilities, substituted provider bytes, and a different Node runtime
fail closed. The evaluator does not call a model or require fleet credentials.

### Install a signed provider release

The release provider includes the compiler closure, NBB runtime files, and the
KCM evaluator/containment runner. It therefore needs neither a compiler checkout
nor a globally installed `nbb`. The installer is plain Node.js, pins the release
signer DID, verifies the descriptor signature and both asset digests, rejects a
different platform or Node binary, and installs into an immutable directory
before atomically updating `current.json`:

```sh
curl -fsSL https://github.com/kotoba-lang/kotoba-fleet/releases/download/kcm-provider-v0.1.0-darwin-arm64/install-kcm-provider.mjs \
  | node --input-type=module - --descriptor \
      https://github.com/kotoba-lang/kotoba-fleet/releases/download/kcm-provider-v0.1.0-darwin-arm64/kotoba-kcm-provider-darwin-arm64.json

~/.local/share/kotoba-kcm/bin/kcm-evaluate \
  --repo ./my-kotoba-project --policy ./kcm-policy.edn \
  --out ./kcm-evaluation.edn
```

For a design-partner pilot with exactly one `.kotoba` entrypoint, no policy
authoring is needed. `--auto` grants only source read, check, and compile;
`--share-out` writes a bounded JSON receipt without source paths, compiler
output tails, or rejection text:

```sh
~/.local/share/kotoba-kcm/bin/kcm-evaluate \
  --repo . --auto --out .kcm/evaluation.edn \
  --share-out .kcm/pilot-share.json
```

Repositories with multiple `.kotoba` files must add `--entry path/to/main.kotoba`;
the evaluator refuses to guess which program the evidence should cover.

Release descriptors are signed by the dedicated Kagi identity
`kcm-provider-release-ed25519-v1`; the installer trusts only
`did:key:z6MknAaLaoj8doPeDrPgszg199YG8kZreH2D3UrWAmLcwgYM`. A signer, descriptor,
archive, manifest, runtime, or installed immutable-release substitution fails
closed. `scripts/sign-kcm-provider-release.cljs` is the release-side producer;
`scripts/install-kcm-provider.mjs` is the standalone consumer.

Pure checks use a cross-session cache keyed by KCM id plus patch digest. A
compiler, dependency, ABI, policy, command, or code change therefore misses;
renames that preserve the admitted definition closure can hit. Effectful KCM
identities are forbidden from declaring a pure/cacheable check. Cache hit/miss
counts and the KCM identity are included in the signed receipt.

Declared compiles are exposed separately as `kotoba_build <id>`. They always
execute because a result-only cache hit would not restore the produced artifact;
the final authoritative checks still run independently before admission.

Seatbelt remains an outer host containment layer, not the language security
model. A container or microVM may replace that backing for high-risk tenants
without changing the KCM contract or guest tool surface. Legacy sandbox specs
remain accepted for migration, but they are reported as `:legacy-sandbox`.

| piece | file | role |
|---|---|---|
| file-backed `:db-api` | `hosts/nbb/fleet/filestore.cljs` | the same append-only contract as `MemStore`, but shared by separate OS processes (ordinal assigned under a lock — the file analogue of `swap!`) |
| sandboxed agent | `hosts/nbb/fleet/sandbox_agent.cljs` | runs **on the node**: KCM typed tools (or the legacy allowlisted test command), exact no-shell Kotoba providers under an OS-level exec backing, enforced budgets, emits a patch |
| KCM contract | `hosts/nbb/fleet/kcm.cljs` | canonical machine/cache identity, closed HostCaps vocabulary, check-argument validation |
| KCM provider | `hosts/nbb/fleet/kcm_provider.cljs` | archive/runtime/manifest/tree verification and exact provider argv |
| admission rules | `hosts/nbb/fleet/gate.cljs` | pure governor rules — lease holder, protected paths, green tests, **exec backing** |
| dispatcher | `bin/fleet-sandbox-dispatch.cljs` | lease → remote run → proposal → gate → materialize → receipt |
| the flow | `hosts/nbb/fleet/dispatch.cljs` | one work-unit end to end — shared by the interactive dispatcher and the tick, so they cannot drift |
| plumbing | `hosts/nbb/fleet/cli.cljs` / `github.cljs` | argv, processes, HTTP; and the one place that talks to GitHub (explicit token or anonymous, never an ambient login) |
| receipts | `hosts/nbb/fleet/receipt.cljs` | the signed decision shape, the sign-off rule, and the published ledger |
| nodes | `hosts/nbb/fleet/node.cljs` + `bin/fleet-nodes.cljs` | measure what each machine can do, and pick one |
| standing tick | `bin/fleet-tick.cljs` | one bounded pass over the work queue |
| shared log | `hosts/nbb/fleet/kotobase_store.cljs` | the same `:db-api` on kotobase.net, so the log is shared across MACHINES |
| agent identity | `hosts/nbb/fleet/identity.cljs` | the dispatcher's own narrow key (kagi) — CACAO auth + signed receipts |
| observer | `bin/fleet-observe.cljs` | read the fleet log from any machine, with no fleet key |
| host selftest | `hosts/nbb/selftest.cljs` | 8 concurrent OS processes race one lease; gate admission matrix; exec-backing containment; identity + receipt signatures; graph derivation; path confinement |

### Landing a patch, and who decides

```bash
--materialize dry-run   # local commits in a scratch repo, pushed nowhere (default)
--materialize push      # a branch + PR on the real repo
--publish               # append the signed receipt to the published ledger
```

`push` needs an explicit `FLEET_PUSH_TOKEN` **and** a work-unit that opted in
(`:allow-push true`). It lands a **branch and a PR**, never a push to main and
never a pin advance: this repo's rule is that advancing a pin is a separate,
verified act, and an agent patch arriving as a reviewable branch keeps that
true. The blobs uploaded are produced by applying the gate-verified patch to
the pinned tree, so what lands is exactly what was judged.

`--publish` appends the signed receipt to `manifest/fleet-sandbox.edn` on the
superproject (`FLEET_LEDGER_TOKEN`), optimistic-locked on the file's blob sha
so two fleets can publish into one ledger without a lock and without either
losing a line. The ledger stays append-only on purpose: the 2026-07-25 policy
that documents show latest state (ADR-2607257000) explicitly keeps append-only
for signed event streams — rewriting a receipt destroys the thing a signature
is for.

Two kinds of path, two different answers:

| in the work-unit | meaning |
|---|---|
| `:protected-paths` | an agent may **never** land here — the gate rejects |
| `:signoff-paths` | allowed, but only behind a human decision — the proposal is **held** |

A held proposal stays pending until someone appends a signed approval
(`--approve <proposal-id>`), which is itself a fact in the log: the approver
signs the proposal id, so an approval cannot be moved to another proposal or
minted by a key the fleet does not recognise. `--drain` then decides it
**without re-running the agent** — re-running would spend a sandbox session
reproducing a patch a human already approved, and might not reproduce it.

### Which machine runs it

```bash
nbb --classpath … bin/fleet-nodes.cljs --nodes a,b,c --requires nbb,git
… bin/fleet-sandbox-dispatch.cljs --work w.edn --node auto --nodes a,b,c
```

A work-unit declares what it NEEDS (`:requires #{:nbb :git}`) and the fleet
picks a machine that has it. Capabilities are measured on the node, including
the strongest one: `contains-exec` runs the sandbox agent's own containment
probe there, so a machine that cannot contain agent-authored code is ineligible
however much toolchain it has. Measured on the mesh today:

```
naphtali  caps=git,nbb,node,sandbox-exec          exec=contained (6 blocked)
asher     caps=clojure,git,jdk,nbb,node,…         exec=contained (6 blocked)
zebulun   caps=clojure,git,jdk,node,sandbox-exec  exec=unprobed   (no nbb)
judah     caps=clojure,git,jdk,node,sandbox-exec  exec=unprobed   (no nbb)
```

### Standing tick

```bash
nbb --classpath … bin/fleet-tick.cljs --work-dir examples --max 3 \
    --store kotobase --db-name fleet-log --nodes naphtali,asher --publish
```

One tick takes up to `--max` units, probes the nodes ONCE, and dispatches each
through the same `fleet.dispatch` flow. It is deliberately bounded rather than a
daemon: every unit is leased, every decision is a signed receipt, and a tick
that dies halfway leaves a lease that expires so the next tick picks the unit
up. Cadence belongs to whatever schedules the tick, not to a process that has
to stay alive.

**Installed schedule** (`deploy/`, macOS side — the fleet still does not
schedule itself):

```bash
cp deploy/run-fleet-tick.cljs ~/.gftd/
sed -e "s|__HOME__|$HOME|g" -e "s|__FLEET_ROOT__|$HOME/github/com-junkawasaki|"     deploy/com.gftd.fleet-tick.plist.tmpl > ~/Library/LaunchAgents/com.gftd.fleet-tick.plist
launchctl load ~/Library/LaunchAgents/com.gftd.fleet-tick.plist
```

Hourly, one unit per tick, reading `~/.gftd/fleet-queue/*.edn`. An empty queue
is a no-op, so installing the timer costs nothing until work is put in it. The
standing policy for UNATTENDED runs is deliberately narrower than the
interactive one:

| | unattended default | why |
|---|---|---|
| materialize | `dry-run` | a scheduled job should not quietly acquire write access to repositories. Pushing is one env var away (`FLEET_TICK_MATERIALIZE=push`) and still needs `:allow-push` on the work-unit plus an explicit `FLEET_PUSH_TOKEN` |
| store | `file` | the shared kotobase log needs the kagi vault, which needs an unlocked keychain a scheduler may not have. `FLEET_TICK_STORE=kotobase` once that is established |
| max units | 1 | the queue drains steadily instead of one tick occupying the fleet |

Verified end to end on the schedule, not just by hand: a queued unit ran under
`launchctl start`, reached a fleet node, came back accepted with a signed
receipt in 59s, and the agent read the vault key fine under launchd.

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

### How capable is the agent, measured

```bash
nbb --classpath … bin/fleet-eval.cljs --tasks examples/eval --reps 3 --node naphtali
```

Four graded tasks against a real repo at a pinned commit, each run N times
through the REAL path — leased work-unit, sandboxed run on a fleet node, the
fleet's own gate — and scored mechanically:

| level | task |
|---|---|
| L1 | one pure helper + its test, two files |
| L2 | a new namespace **and** its test namespace, wired into the suite so it actually runs (three files, one of which the prompt does not name) |
| L3 | extend an existing function with an option, leaving every current caller's behaviour byte-identical |
| L4 | add a new rule inside existing multi-reason logic, unchanged when the new input is absent |

Each run is scored on two independent axes, because they fail differently:
**accepted** (the fleet's gate took it: applies to the pin, tests green on the
node, no protected path) and **meets-spec** (`fleet.eval` checks the diff
against the task's `:expect` — which files, which lines added). Both must hold.
The checker is pure and reads only ADDED lines, so a patch cannot pass by
deleting the thing it was asked to add, and no model judges another model.

Pass rate is reported over **attempts, not best-of-N**: a fleet gets one
attempt per work-unit, so a task that succeeds one time in three is a task that
fails two thirds of the time.

Measured results live in `examples/eval/results/` — read them there rather than
trusting a number quoted in prose.

**The window was the ceiling, and it was raised.** The first run (2026-07-25,
`murakumo-main` = qwen3.6-35b-a3b, 12 attempts) scored L1 3/3, L2 2/3, L3 0/3,
L4 0/3 — and the dominant failure was not reasoning but the serving window:
**8192 tokens**, prompt and generation counted together, so every L3 attempt was
refused at turn 0 before the model saw the task. The agent learned to elide old
tool results and retry a refusal by freeing history; that kept sessions alive
but L3 stayed 0/3, which made it look like a model ceiling.

It was not. The inference head was running `--ctx-size 8192` while `infer.edn`
had documented `:infer/ctx 262144` for weeks. Raised to
`--ctx-size 262144 --parallel 4` = **65536 per slot** (murakumo
`deploy/llama-server.service`), and re-measured:

| level | 8k window | 64k window |
|---|---|---|
| L1 one helper + test | 3/3 | — |
| L2 new ns wired into the suite | 2/3 | — |
| L3 extend an existing fn, keep 5 tests green | **0/3** | **2/3** |
| L4 new rule inside multi-reason logic | 0/3 | 0/3 |

L3 went from "never reached the work" to passing in 6 turns. L4 still fails —
now on the work itself (red tests, or a patch that skips the test file), never
on the window — so that one is a genuine capability limit of this model.

The agent no longer assumes its window: it asks the endpoint at startup
(`--ctx-probe`) and trims against what the server actually reports, because a
hardcoded 8192 survived the upgrade and would have kept eliding history it had
room for.

### Which agent runtime is canonical

**nbb** (owner decision, 2026-07-25). The production `run` is
`hosts/nbb/fleet/sandbox_agent.cljs` on a fleet node — not the JVM
`kotoba-code` session the seam originally named. This is also the runtime this
repo prefers: the priority chain is kotoba wasm → clojurewasm → ClojureScript →
nbb, with the JVM demoted to a last resort.

`kotoba-code` is not retired and not rewritten: it remains a JVM-side
implementation of the same durable-loop design (ADR-2606280001), useful where a
JVM toolchain is already the point. It is simply no longer what the fleet runs.

What the nbb agent still does NOT carry over from that design: a session is one
bounded run with no checkpoint, so a sandbox that dies mid-session is redone
from the top rather than resumed. The lease makes that safe (the unit is picked
up again after the TTL) but not cheap. Porting tick-level checkpointing is the
open item — naming the canonical runtime is what makes it worth doing once
instead of twice.

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
node scripts/kcm-provider-installer-selftest.mjs  # signed descriptor trust boundary
nbb --classpath src:hosts/nbb hosts/nbb/selftest.cljs   # nbb host invariants
```

`.cljc` keeps `edn`/`Exception` `#?(:clj …/:cljs …)`-conditional so the core runs
on JVM, ClojureScript, and the kotoba-clj WASM pod alike.

## Status

F0 scaffold (ADR-2606302000 maturity F0–F1): datom schema + the three loops +
contract tests on `MemStore`. Follow-ups: bind to `kotoba-db` XRPC, integrate
the nbb sandbox agent as the per-agent runtime, and a `murakumo` fleet-view extension.
