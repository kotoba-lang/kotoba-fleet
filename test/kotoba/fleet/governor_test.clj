(ns kotoba.fleet.governor-test
  (:require [clojure.test :refer [deftest is testing]]
            [kotoba.fleet.core :as core]
            [kotoba.fleet.governor :as gov]
            [kotoba.fleet.store :as store]))

(deftest record-drains-proposal
  (testing "record! receipts a proposal so it is no longer pending (drains once)"
    (let [db  (store/mem-store)
          pid (gov/submit-proposal! db {:work "w" :agent "A" :payload 1})]
      (is (= 1 (count (gov/pending-proposals db))) "pending before receipt")
      (gov/record! db {:proposal-id pid :work "w" :verdict :accepted})
      (is (empty? (gov/pending-proposals db)) "no longer pending after receipt"))))

(deftest gate-accepts-and-rejects
  (testing "governor materializes accepted proposals, rejects the rest"
    (let [db        (store/mem-store)
          wrote     (atom [])]
      (gov/submit-proposal! db {:work "w1" :agent "A" :payload {:ok true}  :now 0})
      (gov/submit-proposal! db {:work "w2" :agent "B" :payload {:ok false} :now 1})
      (let [receipts (gov/drain! db {:gate        #(get-in % [:proposal/payload :ok])
                                     :materialize #(swap! wrote conj (:proposal/work %))
                                     :now 2})]
        (is (= 2 (count receipts)))
        (is (= ["w1"] @wrote) "only the accepted proposal is materialized")
        (is (= #{:accepted :rejected} (set (map :receipt/verdict receipts))))))))

(deftest single-writer-serialization
  (testing "proposals drain once, in causal order; re-draining is a no-op"
    (let [db    (store/mem-store)
          order (atom [])]
      (gov/submit-proposal! db {:work "w" :agent "A" :payload 1 :now 0})
      (gov/submit-proposal! db {:work "w" :agent "B" :payload 2 :now 1})
      (gov/drain! db {:gate (constantly true)
                      :materialize #(swap! order conj (:proposal/payload %))
                      :now 2})
      (is (= [1 2] @order) "drained in submit order (single writer)")
      (is (empty? (gov/pending-proposals db)) "nothing pending after drain")
      (gov/drain! db {:gate (constantly true)
                      :materialize #(swap! order conj (:proposal/payload %))
                      :now 3})
      (is (= [1 2] @order) "second drain materializes nothing (idempotent)"))))

(deftest materialize-error-rejects-not-crashes
  (testing "a throwing materialize is caught and recorded as a rejection"
    (let [db (store/mem-store)]
      (gov/submit-proposal! db {:work "w" :agent "A" :payload :boom :now 0})
      (let [[r] (gov/drain! db {:gate (constantly true)
                                :materialize (fn [_] (throw (ex-info "boom" {})))
                                :now 1})]
        (is (= :rejected (:receipt/verdict r)))
        (is (re-find #"materialize-error" (:receipt/reason r)))))))

(deftest tick-produces-view
  (testing "one tick drains and returns a fleet snapshot"
    (let [db (store/mem-store)]
      (gov/submit-proposal! db {:work "w" :agent "A" :payload 1 :now 0})
      (let [{:keys [receipts view]} (core/tick! db {:now 1})]
        (is (= 1 (count receipts)))
        (is (= 1 (:now view)) "snapshot stamped with tick time")
        (is (= 0 (:pending view)) "no proposals left pending")
        (is (pos? (:datoms view)))))))
