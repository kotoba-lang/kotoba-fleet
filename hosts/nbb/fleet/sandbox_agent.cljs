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
  3. The only command the agent can run is the spec's `:test-cmd`. There is no
     generic shell tool, so the model cannot widen its own authority.
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
(def spec
  (if selftest?
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
(def budget    (merge {:max-turns 10 :max-tool-calls 30 :max-tokens 1600
                       :deadline-ms 900000 :test-timeout-ms 300000
                       :max-file-bytes 60000 :max-patch-bytes 400000
                       :max-write-bytes 4000 :max-model-errors 2}
                      (:budget spec)))
(def deadline (+ (js/Date.now) (:deadline-ms budget)))

(defn- log [& xs] (binding [*print-fn* *print-err-fn*] (apply println xs)))
(defn- sh [cmd args opts]
  (cp/execFileSync cmd (clj->js args)
                   (clj->js (merge {:encoding "utf8" :maxBuffer (* 32 1024 1024)} opts))))

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
  (let [g (fn [& args] (sh "git" (concat ["-C" work-dir] args) {}))]
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
        res (try {:exit 0
                  :out (sh "sh" ["-c" test-cmd] {:cwd work-dir
                                                 :timeout (:test-timeout-ms budget)
                                                 :stdio ["ignore" "pipe" "pipe"]})}
                 (catch :default e
                   {:exit (or (.-status e) 1)
                    :out (str (some-> (.-stdout e) str) "\n" (some-> (.-stderr e) str))}))
        tail (->> (str/split-lines (str (:out res))) (take-last 25) (str/join "\n"))]
    (swap! test-runs conj {:exit (:exit res) :ms (- (js/Date.now) started) :tail tail})
    (str "exit=" (:exit res) "\n" tail)))

;; `sh -c <test-cmd>` is the ONE command the agent can reach, and only with the
;; dispatcher-supplied string — the model never supplies a command.
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

(defn- chat! [messages]
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
                               (.push messages
                                      #js {:role "user"
                                           :content (str "Your last request failed at the "
                                                         "server: " (subs (str (.-message e)) 0 300)
                                                         ". This usually means a tool-call "
                                                         "argument was too large. Retry with "
                                                         "edit_file and a short anchor.")})
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
  (sh "git" ["-C" work-dir "add" "-A"] {})
  (let [p (sh "git" ["-C" work-dir "diff" "--binary" "HEAD"] {})]
    (when (> (count p) (:max-patch-bytes budget))
      (throw (ex-info (str "patch exceeds budget: " (count p) " bytes") {})))
    p))

(defn -main []
  (log "sandbox:" (:work-id spec) "on" (str/trim (sh "hostname" [] {})))
  (extract!)
  (let [outcome (try (run-loop!) (catch :default e {:stop :error :error (.-message e) :turns 0}))
        ;; a final authoritative test run — the gate must not trust the model's
        ;; word that it went green
        final-test (t-run-tests {})
        diff (patch!)
        result {:work-id (:work-id spec)
                :node (str/trim (sh "hostname" [] {}))
                :model model
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

(if selftest? (-selftest) (-main))
