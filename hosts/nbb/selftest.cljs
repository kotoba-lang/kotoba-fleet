#!/usr/bin/env nbb
(ns selftest
  "Host-layer selftest: does `fleet.filestore` still satisfy the invariants the
  portable `.cljc` assumes once the log lives in a FILE and the writers are
  separate OS processes?

  The interesting one is the ordinal race. `store.cljc` documents that a
  two-step `next-t` + `transact!` produced only 23-37 distinct ordinals out of
  50 concurrent claims on an atom, which hands the same exclusive lease to two
  agents. The file host has to reproduce swap!'s atomicity with a lock, so this
  spawns REAL concurrent nbb processes (not threads in one runtime) against one
  log file and asserts exactly one winner.

  Run:  nbb --classpath src:hosts/nbb hosts/nbb/selftest.cljs"
  (:require ["node:fs" :as fs]
            ["node:os" :as os]
            ["node:path" :as path]
            ["node:child_process" :as cp]
            ["node:crypto" :as crypto]
            [clojure.string :as str]
            [cljs.reader :as reader]
            [fleet.filestore :as filestore]
            [fleet.gate :as gate]
            [fleet.identity :as fid]
            [fleet.kotobase-store :as kbs]
            [fleet.eval :as ev]
            [fleet.node :as fnode]
            [fleet.receipt :as receipt]
            [kotoba.fleet.agent :as agent]
            [kotoba.fleet.governor :as gov]
            [kotoba.fleet.lease :as lease]
            [kotoba.fleet.store :as store]))

(def argv (vec (.-argv js/process)))
(defn- opt [flag] (second (drop-while #(not= flag %) argv)))

(def failures (atom 0))
(defn- check [label ok? & [detail]]
  (println (str (if ok? "  ok   " "  FAIL ") label (when detail (str " — " detail))))
  (when-not ok? (swap! failures inc)))

;; ---------------------------------------------------------------------------
;; child mode: one process, one claim (used by the concurrency check)

(when-let [child (opt "--claim-child")]
  (let [db (filestore/file-store (opt "--log"))
        r (lease/claim! db {:work "race/unit" :agent child :ttl-ms 60000 :now (js/Date.now)})]
    (println (pr-str (assoc r :agent child)))
    (js/process.exit 0)))

;; ---------------------------------------------------------------------------

(def tmp (fs/mkdtempSync (path/join (os/tmpdir) "fleet-selftest-")))

(println "1. :db-api contract on a file-backed log")
(let [db (filestore/file-store (path/join tmp "a.edn"))]
  (agent/enqueue! db {:unit "u/1" :created-by "t"})
  (check "work is visible as open" (= 1 (count (agent/open-work db 1000))))
  (let [c (lease/claim! db {:work "u/1" :agent "a" :ttl-ms 1000 :now 1000})]
    (check "claim wins on a free unit" (:ok c)))
  (check "holder is the claimant" (= "a" (lease/holder db "u/1" 1000)))
  (check "lease expires by TTL" (nil? (lease/holder db "u/1" 2500)))
  (check "log survives a fresh store handle over the same file"
         (let [db2 (filestore/file-store (path/join tmp "a.edn"))]
           (= "a" (lease/holder db2 "u/1" 1000)))))

(println "\n2. cross-PROCESS lease race (8 concurrent nbb processes, one unit)")
(let [log (path/join tmp "race.edn")
      _ (filestore/file-store log)
      ;; children must load the SAME classpath as this process — the host now
      ;; pulls in the cacao / ed25519 / langchain libs, and a child that cannot
      ;; load them just dies silently and looks like a lost race
      cp-arg (or (second (drop-while #(not= "--classpath" %) argv)) "src:hosts/nbb")
      cmd (str "for i in 1 2 3 4 5 6 7 8; do "
               "nbb --classpath '" cp-arg "' hosts/nbb/selftest.cljs "
               "--claim-child agent-$i --log " log " & done; wait")
      out (cp/execFileSync "sh" #js ["-c" cmd] #js {:encoding "utf8" :maxBuffer 8388608})
      results (->> (str/split-lines out) (remove str/blank?) (mapv reader/read-string))
      db (filestore/file-store log)
      ts (->> (store/datoms db) (filter #(= :lease/t (second %))) (map #(nth % 2)))]
  (check "all 8 processes reported" (= 8 (count results)) (str (count results)))
  (check "exactly one process won the lease"
         (= 1 (count (filter :ok results)))
         (str "winners=" (mapv :agent (filter :ok results))))
  (check "every claim got a distinct ordinal (no TOCTOU collision)"
         (= (count ts) (count (distinct ts)))
         (str (count (distinct ts)) "/" (count ts) " distinct"))
  (check "all processes agree on the holder"
         (= 1 (count (distinct (map :holder results))))
         (str (distinct (map :holder results)))))

(println "\n3. governor is the only writer, receipts are append-only")
(let [db (filestore/file-store (path/join tmp "gov.edn"))
      writes (atom [])]
  (agent/enqueue! db {:unit "u/2" :created-by "t"})
  (agent/claim-and-propose! db {:unit "u/2" :agent "a" :ttl-ms 60000 :now 1000
                                :run (fn [_] {:diff "d" :ok true})})
  ;; a second agent proposes WITHOUT holding the lease (the case the gate exists for)
  (gov/submit-proposal! db {:work "u/2" :agent "intruder" :payload {:diff "x"}})
  (check "two proposals pending" (= 2 (count (gov/pending-proposals db))))
  (let [receipts (gov/drain! db {:gate (fn [p] (= (:proposal/agent p)
                                                  (lease/holder db (:proposal/work p) 1000)))
                                 :materialize (fn [p] (swap! writes conj (:proposal/agent p)))})]
    (check "one accepted, one rejected"
           (= [:accepted :rejected] (mapv :receipt/verdict receipts))
           (pr-str (mapv :receipt/verdict receipts)))
    (check "only the lease holder's payload was materialized" (= ["a"] @writes))
    (check "nothing pending after the drain" (empty? (gov/pending-proposals db)))
    (check "re-draining does not re-materialize"
           (do (gov/drain! db {:gate (constantly true)
                               :materialize (fn [p] (swap! writes conj :again))})
               (= ["a"] @writes)))))

(println "\n4. the governor admits isolation, it does not take the node's word")
(let [ok {:diff "diff --git a/src/x.cljc b/src/x.cljc\n+1\n" :pin "abc"
          :exec-backing :sandbox-exec :exec-probe {:leaked []}
          :stop :model-done :tests {:final-exit 0}}
      ctx {:payload ok :agent "a" :holder "a" :pin "abc"
           :protected-paths ["manifest/"] :allow-unsandboxed? false :patch-applies? true}
      why (fn [m] (gate/reasons (merge ctx m)))]
  (check "a clean sandboxed proposal is admissible" (empty? (why {})))
  (check "unsandboxed exec is rejected even with green tests"
         (some #(str/includes? % "not :sandbox-exec")
               (why {:payload (assoc ok :exec-backing :none)})))
  (check "a backing that leaked during its own probe is rejected"
         (some #(str/includes? % "leaked")
               (why {:payload (assoc ok :exec-probe {:leaked [:network-curl]})})))
  (check "the work-unit can opt out explicitly, and only explicitly"
         (empty? (why {:payload (assoc ok :exec-backing :none) :allow-unsandboxed? true})))
  (check "a non-holder is rejected" (seq (why {:holder "b"})))
  (check "a protected path is rejected"
         (seq (why {:payload (assoc ok :diff "diff --git a/manifest/west.yml b/manifest/west.yml\n+x\n")})))
  (check "red tests are rejected" (seq (why {:payload (assoc ok :tests {:final-exit 1})})))
  (check "a patch that does not apply is rejected" (seq (why {:patch-applies? false}))))

(println "\n5. exec backing contains agent-authored code")
(let [probe (reader/read-string
             (cp/execFileSync "nbb" #js ["hosts/nbb/fleet/sandbox_agent.cljs" "--exec-probe"]
                              #js {:encoding "utf8"}))]
  (check "backing is sandbox-exec" (= :sandbox-exec (:backing probe)) (pr-str (:backing probe)))
  (check "every escape is blocked (home read, ssh keys, network, outside write)"
         (empty? (:leaked probe))
         (str "blocked=" (pr-str (:blocked probe)) " leaked=" (pr-str (:leaked probe)))))

(println "\n6. fleet agent identity + signed receipts (throwaway PEM, not the kagi key)")
(let [pem-path (path/join tmp "agent.pem")
      kp (crypto/generateKeyPairSync
          "ed25519" #js {:privateKeyEncoding #js {:format "pem" :type "pkcs8"}
                         :publicKeyEncoding #js {:format "der" :type "spki"}})
      _ (fs/writeFileSync pem-path (.-privateKey kp))
      me (fid/resolve-identity (str "pem:" pem-path))
      body (pr-str {:receipt/id "r1" :receipt/verdict :accepted})
      cid (fid/sha256-hex body)
      sig (fid/sign-hex me cid)]
  (check "DID is derived from the key, not configured"
         (str/starts-with? (:did me) "did:key:z6Mk") (:did me))
  (check "a receipt signature verifies against the DID alone"
         (fid/verify-hex (:did me) cid sig))
  (check "a tampered receipt is rejected"
         (not (fid/verify-hex (:did me) (fid/sha256-hex (str body "!")) sig)))
  (check "another key cannot pass as this signer"
         (let [other (do (fs/writeFileSync (path/join tmp "other.pem")
                                           (.-privateKey (crypto/generateKeyPairSync
                                                          "ed25519" #js {:privateKeyEncoding #js {:format "pem" :type "pkcs8"}
                                                                         :publicKeyEncoding #js {:format "der" :type "spki"}})))
                         (fid/resolve-identity (str "pem:" (path/join tmp "other.pem"))))]
           (not (fid/verify-hex (:did me) cid (fid/sign-hex other cid))))))

(println "\n7. kotobase graph identity (offline)")
(check "canonical-graph matches the edge's derivation (golden vector)"
       (= "bafyreihzrcxrk34ffo56uetcw7lyioslfulczqh5zpqwy5vok54vkwdfwm"
          (kbs/canonical-graph "did:key:z6MkhnmvngYxx3h13RsVyizeSXwDp54kkWfc87w8yVuLffBa"
                               "fleet-log"))
       (kbs/canonical-graph "did:key:z6MkhnmvngYxx3h13RsVyizeSXwDp54kkWfc87w8yVuLffBa" "fleet-log"))
(check "a different DID gets a different graph (tenant isolation is structural)"
       (not= (kbs/canonical-graph "did:key:z6MkhnmvngYxx3h13RsVyizeSXwDp54kkWfc87w8yVuLffBa" "fleet-log")
             (kbs/canonical-graph "did:key:z6MkhKgm9LbcxSN5uVDBJAhZ3B7s31MHXfS2omgkmCnWu3Ve" "fleet-log")))

(println "\n8. signed receipts, sign-off and node eligibility")
(let [pem (path/join tmp "receipt.pem")
      _ (fs/writeFileSync pem (.-privateKey (crypto/generateKeyPairSync
                                             "ed25519" #js {:privateKeyEncoding #js {:format "pem" :type "pkcs8"}
                                                            :publicKeyEncoding #js {:format "der" :type "spki"}})))
      me (fid/resolve-identity (str "pem:" pem))
      b (receipt/body {:receipt {:receipt/work "u" :receipt/proposal "p1" :receipt/verdict :accepted}
                       :payload {:repo "o/r" :pin "abc" :diff "d" :exec-backing :sandbox-exec
                                 :tests {:final-exit 0} :source-auth :none}
                       :work-id "w1" :node "n1" :at "2026-07-25T00:00:00Z"})
      signed (receipt/sign me b)]
  (check "a signed receipt verifies" (receipt/verify signed))
  (check "a receipt with an edited body does not verify"
         (not (receipt/verify (assoc-in signed [:receipt :fleet.sandbox/verdict] :rejected))))
  (check "a receipt with a swapped cid does not verify"
         (not (receipt/verify (assoc signed :cid (fid/sha256-hex "other")))))
  (check "the ledger round-trips a signed receipt"
         (= [signed] (receipt/read-ledger (str (pr-str signed) "\n"))))

  (check "a patch touching a sign-off path is held"
         (receipt/needs-signoff? ["test/a_test.cljc" "src/a.cljc"] ["test/"]))
  (check "a patch outside sign-off paths is not held"
         (not (receipt/needs-signoff? ["src/a.cljc"] ["test/"])))
  (let [ds (receipt/approval-datoms me "p1" 0)]
    (check "a valid approval from an enrolled DID counts"
           (receipt/approved? ds "p1" [(:did me)]))
    (check "an approval from an unenrolled DID does not count"
           (not (receipt/approved? ds "p1" ["did:key:z6MkOTHER"])))
    (check "an approval cannot be moved to another proposal"
           (not (receipt/approved? (mapv (fn [[e a v t]]
                                           (if (= a :approval/proposal) [e a "p2" t] [e a v t])) ds)
                                   "p2" [(:did me)])))))

(let [caps [{:node "down" :up false}
            {:node "leaky" :up true :caps #{:nbb :git} :contains-exec false}
            {:node "good" :up true :caps #{:nbb :git} :contains-exec true}
            {:node "jvm" :up true :caps #{:nbb :git :jdk} :contains-exec true}]]
  (check "an unreachable node is not chosen"
         (not= "down" (:node (fnode/choose caps {:requires #{:nbb}}))))
  (check "a node whose exec backing leaks is not chosen"
         (= "good" (:node (fnode/choose caps {:requires #{:nbb}}))))
  (check "a capability requirement narrows the choice"
         (= "jvm" (:node (fnode/choose caps {:requires #{:jdk}}))))
  (check "no eligible node explains itself"
         (str/includes? (:why (fnode/choose caps {:requires #{:cuda}})) "no node satisfies")))

(println "\n9. the benchmark checker cannot be talked into a pass")
(let [expect {:files-subset-of ["src/a.cljc" "test/a_test.cljc"]
              :must-touch ["src/a.cljc" "test/a_test.cljc"] :max-files 2
              :must-add ["defn wanted" "deftest"]}
      good (str "diff --git a/src/a.cljc b/src/a.cljc\n+(defn wanted [x] x)\n"
                "diff --git a/test/a_test.cljc b/test/a_test.cljc\n+(deftest t)\n")]
  (check "a patch that does the task passes" (:meets-spec (ev/check good expect)))
  (check "a patch that only claims it in a DELETED line fails"
         (not (:meets-spec (ev/check (str "diff --git a/src/a.cljc b/src/a.cljc\n-(defn wanted [x] x)\n"
                                          "diff --git a/test/a_test.cljc b/test/a_test.cljc\n+(deftest t)\n")
                                     expect))))
  (check "a patch that skips a required file fails"
         (not (:meets-spec (ev/check "diff --git a/src/a.cljc b/src/a.cljc\n+(defn wanted [x] x)\n" expect))))
  (check "a patch that sprays extra files fails"
         (not (:meets-spec (ev/check (str good "diff --git a/other.cljc b/other.cljc\n+x\n") expect))))
  (check "an empty patch fails" (not (:meets-spec (ev/check "" expect))))
  (check "a gate rejection fails the run even when the patch meets the spec"
         (not (:pass (ev/score-run {:task "t" :rep 1 :expect expect
                                    :payload {:diff good} :verdict :rejected}))))
  (check "pass rate is over attempts, not best-of-N"
         (= 1 (get-in (ev/summarize [{:task "t" :pass true} {:task "t" :pass false}
                                     {:task "t" :pass false}])
                      [:tasks "t" :passed]))))

(println "\n10. sandbox path confinement")
(let [out (cp/execFileSync "nbb" #js ["hosts/nbb/fleet/sandbox_agent.cljs" "--selftest"]
                           #js {:encoding "utf8"})]
  (print out)
  (when-not (str/includes? out "confinement ok") (swap! failures inc)))

(println (str "\n" (if (zero? @failures) "ALL CHECKS PASSED" (str @failures " CHECK(S) FAILED"))))
(js/process.exit (if (zero? @failures) 0 1))
