#!/usr/bin/env nbb
(ns fleet.sandbox-agent
  "Sandboxed coding-agent turn — runs ON a murakumo fleet node, never on the
  operator's machine (ADR-2606302000 F3 seam: `run` for kotoba.fleet.agent).

  Contract with the dispatcher: read one EDN spec, produce ONE patch, print it
  as EDN between sentinels. It is deliberately self-contained (node builtins +
  cljs.reader only) because it is scp'd to the node and run standalone.

  Sandbox invariants — all enforced here, not asked of the model:
  1. Work happens in an ephemeral `<root>/work` extracted from a tarball of an
     EXACT pinned commit fetched by the dispatcher from the GitHub API. The
     node never sees, and cannot reach, the operator's checkouts.
  2. Every tool path is resolved and confined under `<root>/work`; `..`,
     absolute paths and symlink escapes are rejected before any I/O.
  3. The only command the agent can run is the spec's `:test-cmd` — but the
     agent can WRITE the code that command executes, so confining the file API
     is not enough on its own. `:test-cmd` therefore runs under an OS-level
     exec backing (macOS Seatbelt via `sandbox-exec`) that denies all network
     access, denies reads of the node's real home (ssh keys, tokens, tailnet
     state), and permits writes only inside the ephemeral sandbox. The backing
     is PROVED at startup by trying those escapes and refusing to run the
     session if any succeeds — a missing or broken profile fails closed, never
     silently downgrades to unsandboxed execution.
  4. The throwaway `git init` inside the sandbox exists ONLY to compute the
     patch (`git diff --binary`). No remote is ever configured, so no push /
     fetch / credential path exists from here.
  5. Budgets (turns, tool calls, wall clock, tokens) are enforced by the loop,
     never trusted to the model, and a blown budget is reported, not hidden.

  The agent CANNOT commit, push, or advance a pin: it emits a patch, the
  dispatcher submits it as a `:proposal/*` datom, and only the single-writer
  governor may materialize it."
  (:require ["node:fs" :as fs]
            ["node:path" :as path]
            ["node:child_process" :as cp]
            [clojure.string :as str]
            [cljs.reader :as reader]))

;; ---------------------------------------------------------------------------
;; spec

(def argv (vec (.-argv js/process)))
(def selftest? (boolean (some #{"--selftest"} argv)))
(def exec-probe? (boolean (some #{"--exec-probe"} argv)))
(def spec
  (if (or selftest? exec-probe?)
    {:work-id "selftest" :root (fs/mkdtempSync "/tmp/fleet-sandbox-selftest-")}
    (let [p (or (second (drop-while #(not= "--spec" %) argv))
                (throw (ex-info "usage: sandbox_agent.cljs --spec <spec.edn>" {})))]
      (reader/read-string (fs/readFileSync p "utf8")))))

(def root      (:root spec))
(def work-dir  (path/join root "work"))
(def tarball   (:tarball spec))
(def test-cmd  (:test-cmd spec))
(def endpoint  (:endpoint spec))
(def model     (:model spec))
(def budget    (merge {:max-turns 10 :max-tool-calls 30 :max-tokens 1200
                       :deadline-ms 900000 :test-timeout-ms 300000
                       :max-file-bytes 60000 :max-patch-bytes 400000
                       :max-write-bytes 4000 :max-model-errors 2
                       ;; The serving endpoint counts prompt + generation against ONE
                       ;; window (measured: n_ctx 8192 on murakumo-main today), so a
                       ;; generous :max-tokens silently halves the room to think in.
                       ;; :ctx-tokens is that window; the loop keeps the prompt under
                       ;; what is left after :max-tokens.
                       :ctx-tokens 8192 :history-floor 2}
                      (:budget spec)))
(def deadline (+ (js/Date.now) (:deadline-ms budget)))

(defn- log [& xs] (binding [*print-fn* *print-err-fn*] (apply println xs)))
(defn- sh [cmd args opts]
  (cp/execFileSync cmd (clj->js args)
                   (clj->js (merge {:encoding "utf8" :maxBuffer (* 32 1024 1024)} opts))))

;; ---------------------------------------------------------------------------
;; 0. exec backing — the boundary for code the AGENT wrote
;;
;; Confining the file tools is not the same as confining execution: the agent
;; writes the test file that `:test-cmd` then runs, so without this every run
;; was arbitrary code execution as the node user (ssh keys, tailnet identity,
;; unrestricted egress). Everything the agent can cause to execute goes through
;; `exec!`.
;;
;; macOS Seatbelt is what every fleet node actually has today (all nodes are
;; macOS 26; none has docker/podman/colima — only lima, with no instance). The
;; backing is a value, not a hardcoded mechanism, so a Linux container backing
;; can be added without touching the loop or the tools.

(def real-home (or (.-HOME js/process.env) "/Users/nobody"))
(def sandbox-home (path/join root "home"))
(def sandbox-tmp (path/join root "tmp"))
(def profile-path (path/join root "sandbox.sb"))
(def exec-backing
  ;; --backing exists so the probe can be run against a DISABLED backing: a
  ;; containment probe that cannot fail proves nothing, so the negative control
  ;; has to be runnable on the same host.
  (or (some-> (second (drop-while #(not= "--backing" %) argv)) keyword)
      (get spec :exec-backing :sandbox-exec)))

(def ^:private profile-src
  ;; SBPL: last matching rule wins, so the allows below deliberately follow the
  ;; broad denies. `(allow default)` keeps dyld/exec/system reads working; the
  ;; three denies are the whole security claim.
  (str "(version 1)\n"
       "(allow default)\n"
       "(deny network*)\n"
       "(deny file-read* (subpath (param \"REAL_HOME\")))\n"
       "(deny file-write*)\n"
       "(allow file-write* (subpath (param \"WORK\")) (subpath (param \"SBHOME\")) (subpath (param \"SBTMP\")))\n"
       "(allow file-write* (literal \"/dev/null\") (literal \"/dev/zero\") (literal \"/dev/random\")\n"
       "                   (literal \"/dev/urandom\") (literal \"/dev/tty\") (literal \"/dev/dtracehelper\"))\n"
       "(allow file-write* (regex #\"^/dev/fd/[0-9]+$\") (regex #\"^/dev/ttys[0-9]*$\"))\n"))

(defn- install-backing! []
  (when (str/starts-with? root real-home)
    ;; the profile denies reads under the node's real home; a sandbox rooted
    ;; there would deny its own workspace
    (throw (ex-info (str "sandbox root must live outside the node's home: " root) {})))
  (fs/mkdirSync sandbox-home #js {:recursive true})
  (fs/mkdirSync sandbox-tmp #js {:recursive true})
  (fs/writeFileSync profile-path profile-src))

(defn exec!
  "Run `cmd` (a shell string) under the exec backing. Returns {:exit :out}.
  This is the ONLY path by which agent-authored code runs."
  [cmd {:keys [timeout-ms]}]
  (let [env (js/Object.assign #js {} js/process.env
                              #js {"HOME" sandbox-home "TMPDIR" sandbox-tmp})
        [prog args] (case exec-backing
                      :sandbox-exec
                      ["sandbox-exec" ["-f" profile-path
                                       "-D" (str "REAL_HOME=" real-home)
                                       "-D" (str "WORK=" work-dir)
                                       "-D" (str "SBHOME=" sandbox-home)
                                       "-D" (str "SBTMP=" sandbox-tmp)
                                       "sh" "-c" cmd]]
                      ;; explicit, recorded in the payload, and rejected by the
                      ;; dispatcher's gate unless the work-unit opts in
                      :none ["sh" ["-c" cmd]]
                      (throw (ex-info (str "unknown :exec-backing " exec-backing) {})))]
    (try {:exit 0 :out (sh prog args {:cwd work-dir :env env
                                      :timeout (or timeout-ms 300000)
                                      :stdio ["ignore" "pipe" "pipe"]})}
         (catch :default e
           {:exit (or (.-status e) 1)
            :out (str (some-> (.-stdout e) str) "\n" (some-> (.-stderr e) str))}))))

(def ^:private probe-marks
  ;; Probing a write escape must not litter the node when the backing is
  ;; deliberately :none (the escapes then really do succeed), so the targets are
  ;; run-specific and removed afterwards.
  [(str real-home "/.fleet-sandbox-probe-" (.-pid js/process))
   (str "/private/tmp/.fleet-sandbox-probe-" (.-pid js/process))])

(def escape-probes
  "What the backing must make impossible. Each entry must EXIT NON-ZERO."
  [{:id :read-home-ssh :cmd (str "ls " real-home "/.ssh")}
   {:id :read-home     :cmd (str "ls " real-home)}
   {:id :network-curl  :cmd "curl -sS --max-time 8 https://example.com -o /dev/null"}
   {:id :network-node  :cmd "node -e \"require('node:https').get('https://example.com',r=>process.exit(0)).on('error',()=>process.exit(9))\""}
   {:id :write-home    :cmd (str "echo pwn > " (first probe-marks))}
   {:id :write-outside :cmd (str "echo pwn > " (second probe-marks))}])

(defn probe-backing
  "Try every escape. Returns {:backing … :blocked [ids] :leaked [ids]}."
  []
  (let [results (doall
                 (for [{:keys [id cmd]} escape-probes]
                   [id (not= 0 (:exit (exec! cmd {:timeout-ms 20000})))]))]
    ;; this process is NOT sandboxed, so it can clean up whatever leaked through
    (doseq [m probe-marks] (try (fs/unlinkSync m) (catch :default _ nil)))
    {:backing exec-backing
     :blocked (mapv first (filter second results))
     :leaked  (mapv first (remove second results))}))

(defn- verify-backing! []
  (let [{:keys [leaked] :as p} (probe-backing)]
    (log "  exec backing:" (name exec-backing)
         "blocked" (count (:blocked p)) "/" (count escape-probes))
    (when (and (seq leaked) (not= :none exec-backing))
      ;; fail closed: a backing that does not hold is not a backing
      (throw (ex-info (str "exec backing failed to contain: " (pr-str leaked)) p)))
    p))

;; ---------------------------------------------------------------------------
;; 1. ephemeral workspace from the pinned tarball

(defn- extract! []
  (sh "rm" ["-rf" work-dir] {})
  (fs/mkdirSync work-dir #js {:recursive true})
  ;; GitHub's tarball API wraps everything in <owner>-<repo>-<sha7>/; a plain
  ;; `git archive` does not. Detect from the actual listing instead of assuming
  ;; — assuming cost ADR-2607178000 a silent false-pass CI receipt.
  (let [entries (->> (sh "tar" ["tzf" tarball] {})
                     str/split-lines (remove str/blank?) (take 40))
        segs    (distinct (map #(first (str/split % #"/")) entries))
        wrapped? (and (= 1 (count segs)) (str/includes? (first entries) "/"))]
    (sh "tar" (concat ["xzf" tarball "-C" work-dir]
                      (when wrapped? ["--strip-components=1"])) {}))
  ;; throwaway local git ONLY as the diff engine (invariant 4)
  (let [g (fn [& args] (sh "git" (concat ["-C" work-dir "-c" "core.hooksPath=/dev/null"]
                                        args) {}))]
    (g "init" "-q")
    (g "-c" "user.email=sandbox@fleet.local" "-c" "user.name=fleet-sandbox" "add" "-A")
    (g "-c" "user.email=sandbox@fleet.local" "-c" "user.name=fleet-sandbox"
       "commit" "-q" "-m" "pinned base")
    (when (seq (str/trim (g "remote"))) (throw (ex-info "sandbox has a remote" {})))))

;; ---------------------------------------------------------------------------
;; 2. confined tools

(defn- safe-path
  "Resolve `p` under the sandbox or throw. Rejects absolute escapes, `..`, and
  symlinks pointing outside (checked on the nearest existing ancestor)."
  [p]
  (when (or (nil? p) (str/blank? p)) (throw (ex-info "empty path" {})))
  (let [abs  (path/resolve work-dir p)
        real-root (fs/realpathSync work-dir)
        anchor (loop [d abs]
                 (if (fs/existsSync d) d (recur (path/dirname d))))
        real-anchor (fs/realpathSync anchor)]
    (when-not (or (= abs work-dir) (str/starts-with? abs (str work-dir path/sep)))
      (throw (ex-info (str "path escapes sandbox: " p) {})))
    (when-not (or (= real-anchor real-root) (str/starts-with? real-anchor (str real-root path/sep)))
      (throw (ex-info (str "path escapes sandbox via symlink: " p) {})))
    abs))

(def tool-calls (atom 0))
(def model-errors (atom 0))
(def test-runs (atom []))

(defn- t-list-files [_]
  (->> (sh "git" ["-C" work-dir "ls-files"] {})
       str/split-lines (remove str/blank?) (take 400) (str/join "\n")))

(defn- t-read-file [{:strs [path]}]
  (let [abs (safe-path path)]
    (if-not (fs/existsSync abs)
      (str "ERROR: no such file: " path)
      (let [s (fs/readFileSync abs "utf8")]
        (if (> (count s) (:max-file-bytes budget))
          (str (subs s 0 (:max-file-bytes budget)) "\n… TRUNCATED")
          s)))))

(defn- check-arg-size [content]
  (when-not (string? content) (throw (ex-info "content must be a string" {})))
  (when (> (count content) (:max-write-bytes budget))
    ;; Not a policy preference: a tool-call argument this large is what made the
    ;; server-side tool-call parser fail mid-session (500, "missing closing
    ;; quote") on the first real run of this PoC. Bounding it here turns a dead
    ;; session into a recoverable tool error.
    (throw (ex-info (str "content is " (count content) " bytes; the limit is "
                         (:max-write-bytes budget)
                         ". Use edit_file for an existing file, or write_file "
                         "then append_file in chunks for a new one.") {}))))

(defn- t-write-file [{:strs [path content]}]
  (let [abs (safe-path path)]
    (check-arg-size content)
    (fs/mkdirSync (path/dirname abs) #js {:recursive true})
    (fs/writeFileSync abs content)
    (str "wrote " path " (" (count content) " bytes)")))

(defn- t-append-file [{:strs [path content]}]
  (let [abs (safe-path path)]
    (check-arg-size content)
    (when-not (fs/existsSync abs) (throw (ex-info (str "no such file: " path) {})))
    (fs/appendFileSync abs content)
    (str "appended " (count content) " bytes to " path)))

(defn- t-edit-file
  "Anchored replacement — delta.op's own Edit semantics: an anchor that is
  missing or ambiguous is an explicit error, never a guess. This keeps tool
  arguments small AND keeps the agent from rewriting (and silently
  reformatting) whole files, which is exactly what polluted the first run's
  patch with unrelated re-indentation hunks."
  [{:strs [path old_string new_string]}]
  (let [abs (safe-path path)]
    (check-arg-size new_string)
    (when-not (string? old_string) (throw (ex-info "old_string must be a string" {})))
    (when-not (fs/existsSync abs) (throw (ex-info (str "no such file: " path) {})))
    (let [content (fs/readFileSync abs "utf8")
          occ (count (re-seq (re-pattern (str/replace old_string #"[.*+?^${}()|\[\]\\]" "\\$&"))
                             content))]
      (cond
        (zero? occ) (str "ERROR: old_string not found in " path)
        (> occ 1) (str "ERROR: old_string is ambiguous in " path " (" occ
                       " occurrences) — include more surrounding lines")
        :else (do (fs/writeFileSync abs (str/replace content old_string new_string))
                  (str "edited " path " (-" (count old_string) "/+"
                       (count new_string) " bytes)"))))))

(defn- t-run-tests [_]
  (let [started (js/Date.now)
        res (exec! test-cmd {:timeout-ms (:test-timeout-ms budget)})
        tail (->> (str/split-lines (str (:out res))) (take-last 25) (str/join "\n"))]
    (swap! test-runs conj {:exit (:exit res) :ms (- (js/Date.now) started) :tail tail})
    (str "exit=" (:exit res) "\n" tail)))

;; The test command is the ONE command the agent can reach, it is supplied by
;; the dispatcher (never by the model), and it runs under the exec backing —
;; so code the agent writes into the test suite is contained too.
(def tools
  {"list_files" t-list-files
   "read_file"  t-read-file
   "write_file" t-write-file
   "append_file" t-append-file
   "edit_file"  t-edit-file
   "run_tests"  t-run-tests})

(def tool-schema
  #js [#js {:type "function"
            :function #js {:name "list_files"
                           :description "List the files tracked in the repository."
                           :parameters #js {:type "object" :properties #js {}}}}
       #js {:type "function"
            :function #js {:name "read_file"
                           :description "Read one file, relative to the repository root."
                           :parameters #js {:type "object"
                                            :properties #js {:path #js {:type "string"}}
                                            :required #js ["path"]}}}
       #js {:type "function"
            :function #js {:name "edit_file"
                           :description "Replace one unique snippet in an existing file. old_string must appear exactly once. Prefer this over write_file."
                           :parameters #js {:type "object"
                                            :properties #js {:path #js {:type "string"}
                                                             :old_string #js {:type "string"}
                                                             :new_string #js {:type "string"}}
                                            :required #js ["path" "old_string" "new_string"]}}}
       #js {:type "function"
            :function #js {:name "write_file"
                           :description "Create a NEW file with the given content (max 4000 bytes; use append_file for the rest)."
                           :parameters #js {:type "object"
                                            :properties #js {:path #js {:type "string"}
                                                             :content #js {:type "string"}}
                                            :required #js ["path" "content"]}}}
       #js {:type "function"
            :function #js {:name "append_file"
                           :description "Append content to an existing file (max 4000 bytes per call)."
                           :parameters #js {:type "object"
                                            :properties #js {:path #js {:type "string"}
                                                             :content #js {:type "string"}}
                                            :required #js ["path" "content"]}}}
       #js {:type "function"
            :function #js {:name "run_tests"
                           :description "Run the repository's test suite. Returns the exit code and output tail."
                           :parameters #js {:type "object" :properties #js {}}}}])

;; ---------------------------------------------------------------------------
;; 3. ReAct loop against the murakumo fleet model

(def system-prompt
  (str "You are a coding agent working inside an isolated sandbox on one checkout "
       "of a repository. Rules:\n"
       "- Use the tools; never claim an edit you did not actually make.\n"
       "- To change an EXISTING file use edit_file with a short unique anchor. "
       "Do not rewrite whole files: unrelated reformatting will get your patch "
       "rejected.\n"
       "- write_file is for creating a NEW file, and each call is capped at "
       "4000 bytes — build a longer file with write_file then append_file.\n"
       "- Make the smallest change that satisfies the task. Do not reformat or "
       "refactor unrelated code.\n"
       "- Run run_tests after editing. If it fails, fix and re-run.\n"
       "- When the tests pass, reply with a one-paragraph summary and STOP. Do "
       "not keep editing after green.\n"
       "- Keep reasoning short."))

(defn- est-tokens
  "Cheap token estimate (~4 chars each). Exact counting would need the server's
  tokenizer; this only has to be conservative enough to trim BEFORE the request
  is refused."
  [messages]
  (quot (count (js/JSON.stringify messages)) 4))

(defn- trim-history!
  "Elide the oldest tool outputs until the prompt fits.

  Tool results are the bulk of a coding session's context and the oldest ones
  are the least useful — the agent has already acted on them. The system
  prompt, the task, and the last `:history-floor` turns are never touched, and
  an elision is announced in place rather than silently dropped so the model
  does not think it read something it can no longer see.

  Measured: at 8192 tokens this failure mode accounted for 5 of 7 benchmark
  failures — every L3 attempt died on it before the model had shown whether it
  could do the task at all."
  [messages]
  (let [budget-tokens (- (:ctx-tokens budget) (:max-tokens budget) 256)
        floor (:history-floor budget)]
    (loop [i 1]
      (when (and (> (est-tokens messages) budget-tokens)
                 (< i (- (.-length messages) (* 2 floor))))
        (let [m (aget messages i)]
          (when (and (= "tool" (.-role m))
                     (not (str/starts-with? (str (.-content m)) "[elided")))
            (set! (.-content m) (str "[elided " (count (str (.-content m)))
                                     " bytes of an earlier tool result — re-read the file if you need it]")))
          (recur (inc i)))))
    messages))

(defn- chat! [messages]
  (trim-history! messages)
  (let [body-file (path/join root "req.json")
        _ (fs/writeFileSync body-file
                            (js/JSON.stringify
                             #js {:model model :messages messages :tools tool-schema
                                  :max_tokens (:max-tokens budget) :temperature 0.2}))
        raw (loop [attempt 0]
              (let [r (try (sh "curl" ["-sS" "--max-time" "300" endpoint
                                       "-H" "content-type: application/json"
                                       "--data-binary" (str "@" body-file)] {})
                           (catch :default e (str "CURL-ERROR " (.-message e))))]
                (if (or (str/starts-with? (str r) "CURL-ERROR") (str/blank? (str r)))
                  (if (< attempt 2) (recur (inc attempt)) (throw (ex-info (str r) {})))
                  r)))
        parsed (js/JSON.parse raw)]
    (when (.-error parsed)
      (throw (ex-info (str "model error: " (js/JSON.stringify (.-error parsed))) {})))
    (-> parsed .-choices (aget 0) .-message)))

(defn- run-loop! []
  (let [messages #js [#js {:role "system" :content system-prompt}
                      #js {:role "user" :content (:prompt spec)}]]
    (loop [turn 0]
      (cond
        (>= turn (:max-turns budget)) {:stop :max-turns :turns turn}
        (> (js/Date.now) deadline)    {:stop :deadline :turns turn}
        :else
        (let [msg (try (chat! messages)
                       (catch :default e
                         ;; A server-side failure (e.g. a tool-call argument the
                         ;; server cannot parse) killed the whole session on the
                         ;; first real run. Feed the error back as guidance and
                         ;; let the agent retry, bounded by :max-model-errors.
                         (if (< @model-errors (:max-model-errors budget))
                           (do (swap! model-errors inc)
                               (log "  model error, recovering:" (.-message e))
                               (when (str/includes? (str (.-message e)) "exceed_context_size")
                                 ;; not a prompting problem: the window is full.
                                 ;; Free the oldest results outright and try again.
                                 (doseq [i (range 1 (max 1 (- (.-length messages) 4)))]
                                   (let [m (aget messages i)]
                                     (when (= "tool" (.-role m))
                                       (set! (.-content m) "[elided to fit the context window]")))))
                               (.push messages
                                      #js {:role "user"
                                           :content (str "Your last request failed at the "
                                                         "server: " (subs (str (.-message e)) 0 300)
                                                         ". If it mentions the context size, keep "
                                                         "your next messages short and do not re-read "
                                                         "files you already read; otherwise a tool-call "
                                                         "argument was too large — retry with edit_file "
                                                         "and a short anchor.")})
                               ::recovered)
                           (throw e))))]
          (if (= ::recovered msg)
            (recur (inc turn))
            (let [calls (or (.-tool_calls msg) #js [])]
              (.push messages msg)
              (if (zero? (.-length calls))
                {:stop :model-done :turns (inc turn) :final (str (.-content msg))}
                (do
                  (doseq [c calls]
                    (let [fname (.. c -function -name)
                          args (try (js->clj (js/JSON.parse (or (.. c -function -arguments) "{}")))
                                    (catch :default _ {}))
                          out (cond
                                (>= @tool-calls (:max-tool-calls budget))
                                "ERROR: tool-call budget exhausted; summarize and stop."

                                (not (contains? tools fname))
                                (str "ERROR: no such tool: " fname)

                                :else
                                (do (swap! tool-calls inc)
                                    (try ((get tools fname) args)
                                         (catch :default e (str "ERROR: " (.-message e))))))]
                      (log "  tool" fname (str "-> " (count (str out)) "B"))
                      (.push messages #js {:role "tool"
                                           :tool_call_id (.-id c)
                                           :content (str out)})))
                  (recur (inc turn)))))))))))

;; ---------------------------------------------------------------------------
;; 4. patch + result

(defn- patch! []
  ;; core.hooksPath=/dev/null: the agent can create .git/hooks/* inside the
  ;; sandbox, and our own git calls must not become an execution path for it.
  (let [g (fn [& args] (sh "git" (concat ["-C" work-dir "-c" "core.hooksPath=/dev/null"]
                                         args) {}))
        _ (g "add" "-A")
        p (g "diff" "--binary" "HEAD")]
    (when (> (count p) (:max-patch-bytes budget))
      (throw (ex-info (str "patch exceeds budget: " (count p) " bytes") {})))
    p))

(defn -main []
  (log "sandbox:" (:work-id spec) "on" (str/trim (sh "hostname" [] {})))
  (extract!)
  (install-backing!)
  (let [probe (verify-backing!)      ; throws before any model call if it leaks
        outcome (try (run-loop!) (catch :default e {:stop :error :error (.-message e) :turns 0}))
        ;; a final authoritative test run — the gate must not trust the model's
        ;; word that it went green
        final-test (t-run-tests {})
        diff (patch!)
        result {:work-id (:work-id spec)
                :node (str/trim (sh "hostname" [] {}))
                :model model
                :exec-backing exec-backing
                :exec-probe probe
                :stop (:stop outcome)
                :error (:error outcome)
                :turns (:turns outcome)
                :tool-calls @tool-calls
                :model-errors @model-errors
                :summary (:final outcome)
                :tests {:runs (count @test-runs)
                        :final-exit (:exit (last @test-runs))
                        :final-tail (:tail (last @test-runs))}
                :diff diff}]
    (println "===FLEET-RESULT-BEGIN===")
    (println (pr-str result))
    (println "===FLEET-RESULT-END===")
    (log "sandbox done:" (:stop outcome) "tests-exit=" (:exit (last @test-runs))
         "diff-bytes=" (count diff))
    (when (str/blank? final-test) nil)))

(defn -selftest
  "Prove the confinement rules hold before any I/O — the sandbox's only real
  defence against a model that asks for ../../.ssh/id_ed25519."
  []
  (fs/mkdirSync (path/join work-dir "src") #js {:recursive true})
  (fs/writeFileSync (path/join work-dir "src/a.txt") "hello")
  (fs/symlinkSync "/etc" (path/join work-dir "escape-link"))
  (let [bad ["../outside.txt" "../../etc/passwd" "/etc/passwd"
             "src/../../outside.txt" "escape-link/passwd" ""]
        rejected (filterv (fn [p] (try (safe-path p) false (catch :default _ true))) bad)
        allowed (try (safe-path "src/a.txt") (safe-path "new/dir/file.txt") true
                     (catch :default _ false))]
    (println (str "  rejected " (count rejected) "/" (count bad) " escapes: " (pr-str rejected)))
    (println (str "  in-sandbox paths allowed: " allowed))
    (if (and (= (count rejected) (count bad)) allowed)
      (println "confinement ok")
      (do (println "confinement FAILED") (js/process.exit 1)))))

(defn -exec-probe
  "Report what the backing blocks on THIS host (used by selftest and to verify
  a fleet node before trusting it with work)."
  []
  (fs/mkdirSync work-dir #js {:recursive true})
  (install-backing!)
  (println (pr-str (probe-backing))))

(cond
  exec-probe? (-exec-probe)
  selftest?   (-selftest)
  :else       (-main))
