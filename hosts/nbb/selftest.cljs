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
            [clojure.string :as str]
            [cljs.reader :as reader]
            [fleet.filestore :as filestore]
            [fleet.gate :as gate]
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
      cmd (str "for i in 1 2 3 4 5 6 7 8; do "
               "nbb --classpath src:hosts/nbb hosts/nbb/selftest.cljs "
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

(println "\n6. sandbox path confinement")
(let [out (cp/execFileSync "nbb" #js ["hosts/nbb/fleet/sandbox_agent.cljs" "--selftest"]
                           #js {:encoding "utf8"})]
  (print out)
  (when-not (str/includes? out "confinement ok") (swap! failures inc)))

(println (str "\n" (if (zero? @failures) "ALL CHECKS PASSED" (str @failures " CHECK(S) FAILED"))))
(js/process.exit (if (zero? @failures) 0 1))
