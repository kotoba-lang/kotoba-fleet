#!/usr/bin/env nbb
(ns fleet-sandbox-dispatch
  "Dispatch one work-unit to a sandboxed coding agent on a murakumo fleet node.

  This is the missing seam of ADR-2606302000: the library already had the
  lease / proposal / governor-drain primitives and named `run` as the injection
  point for 'a kotoba-code session in prod' — this host injects a *remote,
  sandboxed* run on murakumo compute (F3/F4), so the agent loop stops depending
  on a terminal on the operator's laptop.

      lease (kotoba.fleet.lease)
        └─ run  = fetch pinned tarball → scp → ssh node → sandbox_agent.cljs
             └─ proposal (kotoba.fleet.governor/submit-proposal!)
                  └─ gate → materialize (single writer) → receipt

  Properties that come from the shape, not from good behaviour:
  - The node gets a tarball of an exact pinned commit fetched fresh from the
    GitHub API — never the operator's checkout, which drifts (ADR-2607170500).
  - The agent returns a PATCH. It holds no credential, has no remote, and
    cannot commit, push, or advance a pin.
  - The governor is the only writer. It re-runs its own checks; the model's
    claim that tests passed is never taken on trust.
  - Losing the lease race is a no-op backoff, so N dispatchers on N machines
    can point at the same log without a lock server.

  Usage:
    nbb --classpath src:hosts/nbb bin/fleet-sandbox-dispatch.cljs \\
        --work examples/work-kotoba-delta.edn [--agent a1] [--log .fleet/log.edn] \\
        [--node asher] [--materialize dry-run|none] [--mock]"
  (:require ["node:fs" :as fs]
            ["node:os" :as os]
            ["node:path" :as path]
            ["node:child_process" :as cp]
            [clojure.string :as str]
            [cljs.reader :as reader]
            [fleet.filestore :as filestore]
            [fleet.gate :as gate]
            [fleet.identity :as fid]
            [fleet.kotobase-store :as kbs]
            [kotoba.fleet.store :as store]
            [kotoba.fleet.agent :as agent]
            [kotoba.fleet.governor :as gov]
            [kotoba.fleet.lease :as lease]
            [kotoba.fleet.view :as view]))

;; ---------------------------------------------------------------------------
;; args

(def argv (vec (.-argv js/process)))
(defn- opt [flag default]
  (or (second (drop-while #(not= flag %) argv)) default))
(defn- flag? [flag] (boolean (some #{flag} argv)))

(def work-file (or (opt "--work" nil)
                   (throw (ex-info "--work <spec.edn> is required" {}))))
(def spec (reader/read-string (fs/readFileSync work-file "utf8")))
(def agent-name (opt "--agent" (str "agent-" (.hostname os) "-" (.-pid js/process))))
(def log-path (opt "--log" (path/resolve ".fleet/log.edn")))
(def node (opt "--node" (:node spec)))
(def materialize-mode (keyword (opt "--materialize" "dry-run")))
(def mock? (flag? "--mock"))
(def store-kind (keyword (opt "--store" "file")))
(def identity-spec (opt "--identity" "kagi:fleet-agent-sandbox-dispatch"))
(def db-name (opt "--db-name" (get spec :db-name "fleet-log")))
;; A remote log's ordinal allocation is not atomic (no CAS in the :db-api
;; contract), so a winner must be re-confirmed after the log settles.
(def settle-ms (js/parseInt (opt "--settle-ms" "1500")))

(def unit (:unit spec))
(def repo (:repo spec))
(def pin (:pin spec))
(def ttl-ms (get spec :ttl-ms 1800000))
(def protected-paths (get spec :protected-paths [".github/" "manifest/" "90-docs/adr/" ".git/"]))
(def state-dir (path/dirname (path/resolve log-path)))
(def alias-url "https://api.murakumo.cloud/infer/models/murakumo-main")

(defn- sh [cmd args & [opts]]
  (cp/execFileSync cmd (clj->js args)
                   (clj->js (merge {:encoding "utf8" :maxBuffer (* 64 1024 1024)} opts))))
(defn- now [] (js/Date.now))
;; core.fsmonitor is enabled in this workspace and its daemon chatters
;; ("could not read IPC response") into every scripted git call — the scratch
;; repos here are throwaway, so opt out rather than pollute the run log.
(defn- git [args & [opts]]
  (sh "git" (concat ["-c" "core.fsmonitor=false"] args) opts))

;; ---------------------------------------------------------------------------
;; model resolution — ADR-2607173100: resolve the alias, never bake a model id.
;; Fallback keeps only the ENDPOINT (so a fleet model switch still propagates).

(defn resolve-model []
  (try
    (let [j (js/JSON.parse (sh "curl" ["-sS" "--max-time" "20" alias-url]))]
      {:model "murakumo-main"
       :endpoint (or (.-endpoint j) "https://infer.murakumo.cloud/v1/chat/completions")
       :alias-for (aget j "alias-for")
       :resolved :alias})
    (catch :default e
      {:model "murakumo-main"
       :endpoint "https://infer.murakumo.cloud/v1/chat/completions"
       :resolved :endpoint-only
       :note (str "alias lookup failed: " (.-message e))})))

;; ---------------------------------------------------------------------------
;; pinned source

(def source-token
  "Explicit, opt-in credential for a private source repo. Deliberately NOT the
  operator's ambient `gh` login: the dispatcher used to shell out to `gh api`,
  which meant every run carried whatever scopes that account happens to hold.
  A public repo needs no credential at all, so the default is none."
  (some-> (.-FLEET_SOURCE_TOKEN js/process.env) str/trim not-empty))

(def source-auth (if source-token :token :none))

(defn fetch-tarball!
  "The pinned commit as a tarball, straight from codeload over HTTPS."
  [dest]
  (let [url (str "https://codeload.github.com/" repo "/tar.gz/" pin)
        args (cond-> ["-sS" "--fail" "--location" "--max-time" "300" "-o" dest url]
               source-token (concat ["-H" (str "authorization: Bearer " source-token)]))]
    (sh "curl" (vec args))
    (when-not (fs/existsSync dest)
      (throw (ex-info (str "could not fetch " repo "@" pin) {})))
    {:bytes (.-size (fs/statSync dest)) :path dest}))

(defn extract-pinned! [tarball dest]
  (sh "rm" ["-rf" dest])
  (fs/mkdirSync dest #js {:recursive true})
  (let [entries (->> (sh "tar" ["tzf" tarball]) str/split-lines (remove str/blank?) (take 40))
        segs (distinct (map #(first (str/split % #"/")) entries))
        wrapped? (and (= 1 (count segs)) (str/includes? (first entries) "/"))]
    (sh "tar" (concat ["xzf" tarball "-C" dest] (when wrapped? ["--strip-components=1"]))))
  dest)

;; ---------------------------------------------------------------------------
;; run: the sandboxed agent turn on a fleet node

(defn run-on-node!
  "Ship the pinned tarball + the sandbox agent to `node`, run one bounded agent
  session there, and bring back its result. Returns the proposal payload (or nil
  when the agent produced no diff — a no-op releases the lease)."
  [tarball]
  (let [{:keys [model endpoint] :as resolved} (resolve-model)
        remote-root (str "/tmp/fleet-sandbox-" (:work-id spec))
        remote-spec {:work-id (:work-id spec)
                     :root remote-root
                     :tarball (str remote-root "/repo.tgz")
                     :prompt (:prompt spec)
                     :test-cmd (:test-cmd spec)
                     :endpoint endpoint
                     :model model
                     :budget (:budget spec)
                     :exec-backing (get spec :exec-backing :sandbox-exec)}
        local-spec (path/join state-dir (str (:work-id spec) "-spec.edn"))
        started (now)]
    (println (str "  model: " model
                  (when (:alias-for resolved) (str " → " (:alias-for resolved)))
                  " (" (name (:resolved resolved)) ")"))
    (fs/writeFileSync local-spec (pr-str remote-spec))
    (sh "ssh" [node (str "rm -rf " remote-root " && mkdir -p " remote-root)])
    (sh "scp" ["-q" tarball (str node ":" remote-root "/repo.tgz")])
    (sh "scp" ["-q" "hosts/nbb/fleet/sandbox_agent.cljs" local-spec
               (str node ":" remote-root "/")])
    (println (str "  running sandbox on " node ":" remote-root " …"))
    (let [out (sh "ssh" [node (str "cd " remote-root
                                   " && nbb sandbox_agent.cljs --spec "
                                   remote-root "/" (path/basename local-spec))]
                  {:stdio ["ignore" "pipe" "inherit"]})
          body (some-> out
                       (str/split #"===FLEET-RESULT-BEGIN===") second
                       (str/split #"===FLEET-RESULT-END===") first)
          result (reader/read-string (str/trim body))]
      ;; the sandbox is ephemeral: nothing is left on the node between runs
      (sh "ssh" [node (str "rm -rf " remote-root)])
      (println (str "  sandbox: backing=" (name (or (:exec-backing result) :?))
                    " blocked=" (count (get-in result [:exec-probe :blocked]))
                    "/" (+ (count (get-in result [:exec-probe :blocked]))
                           (count (get-in result [:exec-probe :leaked])))
                    "\n  sandbox: stop=" (name (or (:stop result) :?))
                    " turns=" (:turns result)
                    " tools=" (:tool-calls result)
                    " tests-exit=" (get-in result [:tests :final-exit])
                    " diff=" (count (:diff result)) "B"
                    " in " (quot (- (now) started) 1000) "s"))
      (when-not (str/blank? (:diff result))
        (assoc result :pin pin :repo repo :node node :agent agent-name
               :source-auth source-auth
               :wall-ms (- (now) started))))))

(defn run-mock!
  "Coordination-only run (no model, no node): proves lease/proposal/gate/receipt
  without spending inference. Produces a patch the gate will legitimately reject
  so the reject path is exercised too when --mock-bad is passed."
  []
  {:stop :mock :turns 0 :tool-calls 0 :pin pin :repo repo :agent agent-name
   :tests {:final-exit 0} :summary "mock payload"
   :diff (if (flag? "--mock-bad")
           "diff --git a/manifest/west.yml b/manifest/west.yml\n@@ -1 +1 @@\n-x\n+y\n"
           "")})

;; ---------------------------------------------------------------------------
;; gate — the governor's own checks (rules live in fleet.gate; this only
;; supplies the facts it cannot compute itself)

(defn make-gate [db pristine]
  (fn [proposal]
    (let [p (:proposal/payload proposal)
          applies? (when (some? pristine)
                     (try (git ["-C" pristine "apply" "--check" "-"] {:input (:diff p)}) true
                          (catch :default _ false)))
          rs (gate/reasons {:payload p
                            :agent (:proposal/agent proposal)
                            :holder (lease/holder db (:proposal/work proposal) (now))
                            :pin pin
                            :protected-paths protected-paths
                            :allow-unsandboxed? (:allow-unsandboxed-exec spec)
                            :patch-applies? applies?})]
      (if (seq rs)
        (do (println (str "  gate: REJECT — " (str/join "; " rs))) false)
        (do (println (str "  gate: accept (" (count (gate/diff-files (:diff p))) " file(s): "
                          (str/join ", " (gate/diff-files (:diff p))) ")"))
            true)))))

;; ---------------------------------------------------------------------------
;; materialize — the single writer. dry-run lands the patch as a real commit in
;; a local scratch repo for human inspection; it never pushes.

(defn make-materialize [tarball]
  (fn [proposal]
    (let [p (:proposal/payload proposal)]
      (case materialize-mode
        :none (println "  materialize: skipped (--materialize none)")
        :dry-run
        (let [dir (path/join state-dir "materialize" (:work-id spec))
              g (fn [& args] (git (concat ["-C" dir] args)))]
          (extract-pinned! tarball dir)
          (g "init" "-q")
          (g "-c" "user.email=fleet@local" "-c" "user.name=fleet-governor" "add" "-A")
          (g "-c" "user.email=fleet@local" "-c" "user.name=fleet-governor"
             "commit" "-q" "-m" (str repo " @ " (subs pin 0 12) " (pinned base)"))
          (git ["-C" dir "apply" "-"] {:input (:diff p)})
          (g "add" "-A")
          (g "-c" "user.email=fleet@local" "-c" "user.name=fleet-governor"
             "commit" "-q" "-m" (str "fleet-agent: " (:unit spec) "\n\n"
                                     (str/trim (or (:summary p) ""))
                                     "\n\nagent: " (:agent p) "\nnode: " (:node p)))
          (println (str "  materialize: " dir " (2 commits; inspect with `git -C "
                        dir " log -p`)")))
        (throw (ex-info (str "unknown --materialize mode: " materialize-mode) {}))))))

;; ---------------------------------------------------------------------------
;; main

(defn open-store!
  "The coordination log. `file` is one machine; `kotobase` is the fleet's own
  tenant graph on kotobase.net, reachable from every machine that holds the
  fleet agent key."
  [me]
  (case store-kind
    :file (do (println (str "  store: file " log-path))
              (filestore/file-store (path/resolve log-path)))
    :kotobase (let [db (kbs/kotobase-store {:identity me :db-name db-name})]
                (println (str "  store: kotobase.net db=" db-name
                              "\n         graph=" (:fleet/graph db)
                              "\n         did=" (:fleet/did db)))
                db)
    (throw (ex-info (str "unknown --store " store-kind) {}))))

(defn sign-receipts!
  "Sign each governor decision with the fleet agent key and append it to the
  log. A receipt is then checkable against the DID enrolled in
  manifest/fleet-agents.edn instead of being believed because of where it was
  found — the same containment the murakumo CI signer has: this key can add to
  an append-only log and nothing else."
  [db me receipts]
  (mapv (fn [r]
          (let [body (pr-str r)
                cid (fid/sha256-hex body)
                sig (fid/sign-hex me cid)]
            (store/transact-with-t!
             db (fn [t]
                  [[(str "sig|" (:receipt/id r)) :sigreceipt/receipt (:receipt/id r) t]
                   [(str "sig|" (:receipt/id r)) :sigreceipt/cid cid t]
                   [(str "sig|" (:receipt/id r)) :sigreceipt/signature sig t]
                   [(str "sig|" (:receipt/id r)) :sigreceipt/signer (:did me) t]
                   [(str "sig|" (:receipt/id r)) :sigreceipt/t t t]]))
            {:receipt r :cid cid :signature sig :signer (:did me)}))
        receipts))

(defn -main []
  (fs/mkdirSync state-dir #js {:recursive true})
  (let [me (fid/resolve-identity identity-spec)
        db (open-store! me)]
    (println (str "unit=" unit " agent=" agent-name " signer=" (:did me)
                  " source-auth=" (name source-auth)))
    (when-not (some #(and (= :work/unit (second %)) (= unit (nth % 2))) (store/datoms db))
      (agent/enqueue! db {:unit unit :created-by agent-name})
      (println "  enqueued work-unit"))
    (let [tarball (when-not mock?
                    (let [dest (path/join state-dir (str (:work-id spec) ".tgz"))]
                      (println (str "  fetching " repo " @ " (subs pin 0 12) " …"))
                      (:path (fetch-tarball! dest))))
          pristine (when-not mock?
                     (extract-pinned! tarball (path/join state-dir "pristine" (:work-id spec))))
          outcome (agent/claim-and-propose!
                   db {:unit unit :agent agent-name :ttl-ms ttl-ms :now (now)
                       ;; a run that throws must RELEASE the lease (the :no-op
                       ;; path) rather than pin the unit until TTL — an ssh
                       ;; timeout is not a crashed agent holding real work.
                       :run (fn [_]
                              (try
                                ;; On a shared remote log the claim is
                                ;; optimistic: re-read after it settles and back
                                ;; off if another dispatcher won, BEFORE burning
                                ;; a sandbox run on work that cannot land.
                                (if-not (kbs/confirmed-holder? db unit agent-name
                                                               (if (= :file store-kind) 0 settle-ms))
                                  (do (println "  claim lost after settle — backing off") nil)
                                  (if mock?
                                    (let [m (run-mock!)]
                                      (when-not (str/blank? (:diff m)) m))
                                    (run-on-node! tarball)))
                                (catch :default e
                                  (println (str "  run FAILED: " (.-message e)))
                                  nil)))})]
      (println (str "  claim: " (name (:status outcome))
                    (when (:holder outcome) (str " (holder=" (:holder outcome) ")"))))
      (when (= :proposed (:status outcome))
        (let [receipts (gov/drain! db {:gate (make-gate db pristine)
                                       :materialize (make-materialize tarball)})
              verdicts (mapv :receipt/verdict receipts)]
          (agent/complete! db {:unit unit :agent agent-name :now (now)})
          (when (some #{:accepted} verdicts) (agent/close-work! db unit))
          (fs/mkdirSync (path/join state-dir "receipts") #js {:recursive true})
          (let [signed (sign-receipts! db me receipts)]
            (fs/writeFileSync (path/join state-dir "receipts" (str (:work-id spec) ".edn"))
                              (str/join "\n" (map pr-str signed)))
            (println (str "  receipts: " (str/join ", " (map name verdicts))
                          " (signed by " (:did me) ")")))))
      (println "\nfleet view:")
      (println (pr-str (view/snapshot db (now)))))))

(-main)
