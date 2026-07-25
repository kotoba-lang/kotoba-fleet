(ns fleet.node
  "Which machine can actually run this work-unit.

  The dispatcher used to take `:node` from the work-unit and hope. That
  assumption broke twice in one afternoon: `asher` stopped answering ssh
  mid-session, and only one node in the mesh has a JVM at all
  (ADR-2607178000 measured 1 of 9). A work-unit should say what it NEEDS —
  nbb, a JVM, a containing exec backing — and the fleet should pick a machine
  that has it, or say plainly that none does.

  A capability is measured, never configured: the probe runs on the node and
  reports what it found. `:contains-exec` is the strongest of them, because it
  runs the sandbox agent's own containment probe there — a node that cannot
  contain agent-authored code is not eligible for agent work, however much
  toolchain it has."
  (:require [clojure.string :as str]
            [cljs.reader :as reader]
            [fleet.cli :as cli]))

(def default-ssh-opts ["-o" "BatchMode=yes" "-o" "ConnectTimeout=8"])

(defn ssh
  "Run a command on `node`. {:exit :out} — an unreachable node is a value, not
  an exception, because 'this machine is down' is ordinary fleet weather."
  [node command & [{:keys [timeout-sec] :or {timeout-sec 120}}]]
  (cli/sh-status "ssh" (concat default-ssh-opts [node command])
                 {:timeout (* 1000 timeout-sec)}))

(defn scp! [node src dest]
  (cli/sh "scp" (concat ["-q"] default-ssh-opts [src (str node ":" dest)])))

(defn probe
  "Measure one node. Returns a capability map; `:up false` when unreachable."
  [node {:keys [agent-path deep?] :or {deep? true}}]
  (let [{:keys [exit out]} (ssh node (str "hostname; "
                                          "command -v nbb >/dev/null && echo has:nbb; "
                                          "command -v node >/dev/null && echo has:node; "
                                          "command -v git >/dev/null && echo has:git; "
                                          "command -v clojure >/dev/null && echo has:clojure; "
                                          "ls /opt/homebrew/opt/openjdk/bin/java >/dev/null 2>&1 && echo has:jdk; "
                                          "ls /usr/bin/sandbox-exec >/dev/null 2>&1 && echo has:sandbox-exec")
                              {:timeout-sec 40})]
    (if-not (zero? exit)
      {:node node :up false :reason (str/trim (str out))}
      (let [caps (set (keep #(second (re-find #"^has:(.+)$" %)) (str/split-lines out)))
            base {:node node :up true
                  :hostname (first (str/split-lines out))
                  :caps (into #{} (map keyword caps))}]
        (if-not (and deep? agent-path (contains? caps "nbb"))
          base
          ;; the containment probe: ship the sandbox agent and ask the node to
          ;; try the escapes itself
          (let [remote (str "/tmp/fleet-probe-" (.-pid js/process) ".cljs")
                _ (try (scp! node agent-path remote) (catch :default _ nil))
                {:keys [exit out]} (ssh node (str "cd /tmp && nbb " remote " --exec-probe; rm -f " remote)
                                        {:timeout-sec 180})
                result (when (zero? exit)
                         (try (reader/read-string (str/trim (last (remove str/blank? (str/split-lines out)))))
                              (catch :default _ nil)))]
            (assoc base
                   :exec-probe result
                   :contains-exec (boolean (and result (empty? (:leaked result)))))))))))

(defn eligible?
  "Does `cap` satisfy `requires` (a set of capability keywords)? Containment is
  required unless the work-unit explicitly opted out of sandboxed exec."
  [cap {:keys [requires allow-unsandboxed?]}]
  (and (:up cap)
       (every? (:caps cap) requires)
       (or allow-unsandboxed? (:contains-exec cap))))

(defn choose
  "First eligible node, in the order given. Returns {:node … :why …} or
  {:node nil :why …} — an empty fleet must explain itself, not return nil."
  [caps req]
  (if-let [hit (first (filter #(eligible? % req) caps))]
    {:node (:node hit) :cap hit}
    {:node nil
     :why (str "no node satisfies " (pr-str (:requires req))
               (when-not (:allow-unsandboxed? req) " + contained exec")
               "; probed: "
               (str/join ", " (map (fn [c]
                                     (str (:node c)
                                          (cond (not (:up c)) " (down)"
                                                (not (:contains-exec c)) " (exec not contained)"
                                                :else (str " (caps " (pr-str (:caps c)) ")"))))
                                   caps)))}))

(defn inventory
  "Node names from a murakumo fleet.edn, or an explicit comma-separated list."
  [{:keys [fleet-edn nodes]}]
  (cond
    (seq nodes) (vec (remove str/blank? (str/split nodes #",")))
    fleet-edn (->> (reader/read-string (cli/sh "cat" [fleet-edn]))
                   :nodes (mapv :name))
    :else []))
