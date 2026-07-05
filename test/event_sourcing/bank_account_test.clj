(ns event-sourcing.bank-account-test
  (:require [clojure.test :refer [deftest is testing]]
            [event-sourcing.aggregate :as agg]
            [event-sourcing.store :as store]
            [event-sourcing.projection :as proj]
            [event-sourcing.sample.bank-account :as bank]))

(deftest open-creates-account-with-initial-deposit
  (let [account (bank/open-account "acc-1" "Alice" 100.0)]
    (is (= "acc-1" (:aggregate-id account)))
    (is (= "Alice" (:account-holder account)))
    (is (= 100.0 (:balance account)))
    (is (= 1 (:version account)))))

(deftest deposit-increases-balance
  (let [account (-> (bank/open-account "acc-1" "Alice" 100.0)
                    (bank/deposit 50.0 :description "Paycheck"))]
    (is (= 150.0 (:balance account)))
    (is (= 2 (:version account)))))

(deftest withdraw-decreases-balance
  (let [account (-> (bank/open-account "acc-1" "Alice" 100.0)
                    (bank/withdraw 30.0 :description "Coffee"))]
    (is (= 70.0 (:balance account)))))

(deftest withdraw-insufficient-funds-throws
  (let [account (bank/open-account "acc-1" "Alice" 50.0)]
    (is (thrown? clojure.lang.ExceptionInfo
                 (bank/withdraw account 100.0)))))

(deftest transfer-decreases-source-balance
  (let [account (-> (bank/open-account "acc-1" "Alice" 200.0)
                    (bank/transfer-to "acc-2" 75.0 :description "Rent"))]
    (is (= 125.0 (:balance account)))))

(deftest receive-transfer-increases-balance
  (let [account (-> (bank/open-account "acc-2" "Bob" 50.0)
                    (bank/receive-transfer "acc-1" 75.0 :description "Rent"))]
    (is (= 125.0 (:balance account)))))

(deftest uncommitted-events-tracks-all-raised-events
  (let [account (-> (bank/open-account "acc-1" "Alice" 100.0)
                    (bank/deposit 50.0)
                    (bank/withdraw 20.0))]
    (is (= 3 (count (agg/get-uncommitted-events account))))))

(deftest clear-uncommitted-events-empties-list
  (let [account (-> (bank/open-account "acc-1" "Alice" 100.0)
                    (bank/deposit 50.0)
                    agg/clear-uncommitted-events)]
    (is (empty? (agg/get-uncommitted-events account)))))

(deftest snapshot-round-trip-restores-state
  (let [account (-> (bank/open-account "acc-1" "Alice" 100.0)
                    (bank/deposit 50.0)
                    (bank/withdraw 20.0))
        snapshot (agg/create-snapshot account)
        restored (agg/restore-from-snapshot (bank/make-bank-account) snapshot)]
    (is (= "acc-1" (:aggregate-id restored)))
    (is (= "Alice" (:account-holder restored)))
    (is (= 130.0 (:balance restored)))
    (is (= 3 (:version restored)))))

(deftest event-store-round-trip
  (testing "Events can be stored and replayed to rebuild aggregate state"
    (let [db-path (str "test_bank_" (System/nanoTime) ".db")
          ds (store/create-datasource db-path)]
      (try
        (store/initialize! ds)
        (let [account (-> (bank/open-account "acc-1" "Alice" 100.0)
                          (bank/deposit 50.0)
                          (bank/withdraw 20.0))
              _ (store/save-aggregate! ds account 0)
              reloaded (store/load-aggregate ds (bank/make-bank-account) "acc-1")]
          (is (= "acc-1" (:aggregate-id reloaded)))
          (is (= "Alice" (:account-holder reloaded)))
          (is (= 130.0 (:balance reloaded)))
          (is (= 3 (:version reloaded)))
          (is (empty? (agg/get-uncommitted-events reloaded))))
        (finally
          (.delete (java.io.File. db-path)))))))

(deftest in-memory-store-round-trip
  (testing "load/save-aggregate work against the in-memory EventStore (no disk)"
    (let [store (store/create-in-memory-store)
          account (-> (bank/open-account "acc-1" "Alice" 100.0)
                      (bank/deposit 50.0)
                      (bank/withdraw 20.0))]
      (store/save-aggregate! store account 0)
      (let [reloaded (store/load-aggregate store (bank/make-bank-account) "acc-1")]
        (is (= "acc-1" (:aggregate-id reloaded)))
        (is (= "Alice" (:account-holder reloaded)))
        (is (= 130.0 (:balance reloaded)))
        (is (= 3 (:version reloaded))))
      ;; A stale expected-version raises the structured concurrency conflict.
      (is (thrown? clojure.lang.ExceptionInfo
                   (store/append-events! store "acc-1"
                                         [{:event-type :x :version 1}] 0))))))

(deftest in-memory-store-projection-catch-up
  (testing "Projections catch up over the in-memory store"
    (let [store (store/create-in-memory-store)
          alice (bank/open-account "acc-1" "Alice" 100.0)
          _ (store/save-aggregate! store alice 0)
          summary (bank/create-account-summary-projection)
          engine (-> (proj/create-projection-engine)
                     (proj/register! summary))]
      (proj/catch-up! engine (fn [after-seq] (store/get-all-events store after-seq)))
      (is (= "Alice" (:account-holder (get @summary "acc-1")))))))

(deftest projection-catches-up-on-events
  (testing "Projections process stored events correctly"
    (let [db-path (str "test_proj_" (System/nanoTime) ".db")
          ds (store/create-datasource db-path)]
      (try
        (store/initialize! ds)
        (let [alice (bank/open-account "acc-1" "Alice" 100.0)
              _ (store/save-aggregate! ds alice 0)
              summary (bank/create-account-summary-projection)
              engine (-> (proj/create-projection-engine)
                         (proj/register! summary))]
          (proj/catch-up! engine (fn [after-seq] (store/get-all-events ds after-seq)))
          (let [alice-summary (get @summary "acc-1")]
            (is (= "Alice" (:account-holder alice-summary)))
            (is (= 100.0 (:balance alice-summary)))
            (is (= 1 (:transaction-count alice-summary)))))
        (finally
          (.delete (java.io.File. db-path)))))))
