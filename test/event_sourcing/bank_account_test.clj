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

(deftest save-with-stale-version-throws-concurrency-conflict
  (testing "append-events!/save-aggregate! reject a stale expected-version"
    (let [db-path (str "test_conflict8_" (System/nanoTime) ".db")
          ds (store/create-datasource db-path)]
      (try
        (store/initialize! ds)
        (store/save-aggregate! ds (bank/open-account "acc-1" "Alice" 100.0) 0)
        (is (thrown? clojure.lang.ExceptionInfo
                     (store/append-events! ds "acc-1"
                                           [{:event-type :money-deposited
                                             :version 1 :amount 5.0}]
                                           0)))
        (finally
          (.delete (java.io.File. db-path)))))))

(deftest snapshot-written-and-used-by-load-fast-path
  (testing "A snapshot at the interval is written and load-aggregate replays only later events"
    (let [db-path (str "test_snap8_" (System/nanoTime) ".db")
          ds (store/create-datasource db-path)]
      (try
        (store/initialize! ds)
        ;; interval 3: open (v1) + 2 deposits (v2,v3) -> snapshot at v3.
        (let [acc (-> (bank/open-account "acc-1" "Alice" 100.0)
                      (bank/deposit 10.0)
                      (bank/deposit 10.0))]
          (store/save-aggregate! ds acc 3))
        (let [snap (store/get-latest-snapshot ds "acc-1")]
          (is (some? snap) "Snapshot exists at the interval boundary")
          (is (= 3 (:version snap))))
        ;; get-events after the snapshot version returns only later events.
        (let [acc (-> (store/load-aggregate ds (bank/make-bank-account) "acc-1")
                      (bank/deposit 5.0))]
          (store/save-aggregate! ds acc 3))
        (is (= 1 (count (store/get-events ds "acc-1" 3)))
            "Only the post-snapshot event is returned for after-version 3")
        (let [reloaded (store/load-aggregate ds (bank/make-bank-account) "acc-1")]
          (is (= 4 (:version reloaded)))
          (is (= 125.0 (:balance reloaded))))
        (finally
          (.delete (java.io.File. db-path)))))))

(deftest get-events-after-version-returns-only-later-events
  (let [db-path (str "test_after8_" (System/nanoTime) ".db")
        ds (store/create-datasource db-path)]
    (try
      (store/initialize! ds)
      (store/save-aggregate! ds (-> (bank/open-account "acc-1" "Alice" 100.0)
                                    (bank/deposit 10.0)
                                    (bank/deposit 20.0)) 0)
      (is (= 3 (count (store/get-events ds "acc-1" 0))))
      (is (= 1 (count (store/get-events ds "acc-1" 2))))
      (is (= 0 (count (store/get-events ds "acc-1" 3))))
      (finally
        (.delete (java.io.File. db-path))))))

(deftest transaction-history-projection-records-all-event-types
  (testing "The transaction-history projection produces ordered records for all five event types"
    (let [tx-history (bank/create-transaction-history-projection)
          engine (-> (proj/create-projection-engine)
                     (proj/register! tx-history))
          events [{:sequence-number 1 :event-type :account-opened
                   :aggregate-id "acc-1" :account-holder "Alice" :initial-deposit 100.0
                   :timestamp "t1"}
                  {:sequence-number 2 :event-type :money-deposited
                   :aggregate-id "acc-1" :amount 50.0 :description "pay" :timestamp "t2"}
                  {:sequence-number 3 :event-type :money-withdrawn
                   :aggregate-id "acc-1" :amount 20.0 :description "coffee" :timestamp "t3"}
                  {:sequence-number 4 :event-type :money-transferred
                   :aggregate-id "acc-1" :amount 30.0 :target-account-id "acc-2"
                   :description "rent" :timestamp "t4"}
                  {:sequence-number 5 :event-type :transfer-received
                   :aggregate-id "acc-1" :amount 15.0 :source-account-id "acc-2"
                   :description "refund" :timestamp "t5"}]]
      (proj/catch-up! engine (fn [_] events))
      (let [records (get @tx-history "acc-1")]
        (is (= 5 (count records)))
        (is (= ["Account Opened" "Deposit" "Withdrawal" "Transfer Out" "Transfer In"]
               (mapv :type records)))
        ;; Withdrawal and transfer-out are recorded as negative amounts.
        (is (= -20.0 (:amount (nth records 2))))
        (is (= -30.0 (:amount (nth records 3))))
        (is (= 15.0 (:amount (nth records 4))))))))

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
