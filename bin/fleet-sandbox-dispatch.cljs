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

(defn fetch-tarball! [dest]
  (let [buf (cp/execFileSync "gh" #js ["api" (str "repos/" repo "/tarball/" pin)]
                             #js {:maxBuffer (* 512 1024 1024)})]
    (fs/writeFileSync dest buf)
    {:bytes (.-length buf) :path dest}))

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
                     :budget (:budget spec)}
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
      (println (str "  sandbox: stop=" (name (or (:stop result) :?))
                    " turns=" (:turns result)
                    " tools=" (:tool-calls result)
                    " tests-exit=" (get-in result [:tests :final-exit])
                    " diff=" (count (:diff result)) "B"
                    " in " (quot (- (now) started) 1000) "s"))
      (when-not (str/blank? (:diff result))
        (assoc result :pin pin :repo repo :node node :agent agent-name
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
;; gate — the governor's own checks. Nothing here trusts the agent.

(defn diff-files [diff]
  (->> (str/split-lines (or diff ""))
       (keep #(second (re-find #"^diff --git a/(\S+) b/" %)))
       distinct))

(defn make-gate [db pristine]
  (fn [proposal]
    (let [p (:proposal/payload proposal)
          files (diff-files (:diff p))
          holder (lease/holder db (:proposal/work proposal) (now))
          reasons
          (cond-> []
            (not= holder (:proposal/agent proposal))
            (conj (str "proposer does not hold the lease (holder=" holder ")"))

            (str/blank? (:diff p)) (conj "empty diff")

            (not= pin (:pin p)) (conj "payload pin != dispatched pin")

            (some (fn [f] (some #(str/starts-with? f %) protected-paths)) files)
            (conj (str "touches a protected path: " (vec files)))

            (not= 0 (get-in p [:tests :final-exit]))
            (conj (str "tests not green on the node (exit="
                       (get-in p [:tests :final-exit]) ")"))

            (= :error (:stop p)) (conj (str "agent errored: " (:error p)))

            (nil? pristine) (conj "no pinned tree to verify the patch against")

            ;; does the patch actually apply to the pinned tree?
            (and (some? pristine)
                 (not (try (git ["-C" pristine "apply" "--check" "-"]
                               {:input (:diff p)}) true
                           (catch :default _ false))))
            (conj "patch does not apply cleanly to the pinned tree"))]
      (if (seq reasons)
        (do (println (str "  gate: REJECT — " (str/join "; " reasons))) false)
        (do (println (str "  gate: accept (" (count files) " file(s): "
                          (str/join ", " files) ")"))
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

(defn -main []
  (fs/mkdirSync state-dir #js {:recursive true})
  (let [db (filestore/file-store (path/resolve log-path))]
    (println (str "unit=" unit " agent=" agent-name " log=" log-path))
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
                                (if mock?
                                  (let [m (run-mock!)]
                                    (when-not (str/blank? (:diff m)) m))
                                  (run-on-node! tarball))
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
          (fs/writeFileSync (path/join state-dir "receipts" (str (:work-id spec) ".edn"))
                            (str/join "\n" (map pr-str receipts)))
          (println (str "  receipts: " (str/join ", " (map name verdicts))))))
      (println "\nfleet view:")
      (println (pr-str (view/snapshot db (now)))))))

(-main)
