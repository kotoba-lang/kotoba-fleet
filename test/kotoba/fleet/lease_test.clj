(ns kotoba.fleet.lease-test
  (:require [clojure.test :refer [deftest is testing]]
            [kotoba.fleet.lease :as lease]
            [kotoba.fleet.store :as store]))

(deftest two-agents-one-holder
  (testing "concurrent claims on the same work-unit yield exactly one holder"
    (let [db (store/mem-store)
          a  (lease/claim! db {:work "w1" :agent "A" :ttl-ms 1000 :now 100})
          b  (lease/claim! db {:work "w1" :agent "B" :ttl-ms 1000 :now 101})]
      (is (:ok a) "earliest claim (A) wins")
      (is (not (:ok b)) "later claim (B) loses without any lock server")
      (is (= "A" (:holder b)) "B observes A as holder")
      (is (= "A" (lease/holder db "w1" 200)) "holder is deterministic"))))

(deftest deterministic-across-observers
  (testing "holder is the earliest active claim regardless of query time"
    (let [db (store/mem-store)]
      (lease/claim! db {:work "w" :agent "X" :ttl-ms 5000 :now 0})
      (lease/claim! db {:work "w" :agent "Y" :ttl-ms 5000 :now 1})
      (is (= "X" (lease/holder db "w" 10)))
      (is (= "X" (lease/holder db "w" 4000))))))

(deftest ttl-reclaim
  (testing "an expired lease is reclaimed; the next claim wins"
    (let [db (store/mem-store)]
      (lease/claim! db {:work "w" :agent "A" :ttl-ms 100 :now 0})
      (is (= "A" (lease/holder db "w" 50)) "A holds within TTL")
      (is (nil? (lease/holder db "w" 100)) "lease expired -> free")
      (let [c (lease/claim! db {:work "w" :agent "B" :ttl-ms 100 :now 100})]
        (is (:ok c) "B wins after A's lease expired")
        (is (= "B" (lease/holder db "w" 120)))))))

(deftest release-frees-work
  (testing "releasing hands the work to the next active claim"
    (let [db (store/mem-store)]
      (lease/claim! db {:work "w" :agent "A" :ttl-ms 9999 :now 0})
      (lease/claim! db {:work "w" :agent "B" :ttl-ms 9999 :now 1})
      (is (= "A" (lease/holder db "w" 10)))
      (lease/release! db {:work "w" :agent "A" :now 20})
      (is (= "B" (lease/holder db "w" 30)) "B takes over after A releases"))))

(deftest disjoint-work-no-contention
  (testing "claims on different work-units never contend"
    (let [db (store/mem-store)]
      (is (:ok (lease/claim! db {:work "w1" :agent "A" :ttl-ms 1000 :now 0})))
      (is (:ok (lease/claim! db {:work "w2" :agent "B" :ttl-ms 1000 :now 0})))
      (is (= "A" (lease/holder db "w1" 5)))
      (is (= "B" (lease/holder db "w2" 5))))))

(deftest append-only-log-grows
  (testing "the log only ever grows (nothing overwritten)"
    (let [db (store/mem-store)
          n0 (count (store/datoms db))]
      (lease/claim! db {:work "w" :agent "A" :ttl-ms 10 :now 0})
      (lease/release! db {:work "w" :agent "A" :now 1})
      (is (> (count (store/datoms db)) n0)))))
