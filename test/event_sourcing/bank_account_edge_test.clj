(ns event-sourcing.bank-account-edge-test
  (:require [clojure.test :refer [deftest is testing]]
            [event-sourcing.aggregate :as agg]
            [event-sourcing.projection :as proj]
            [event-sourcing.sample.bank-account :as bank]))

;; --- Validation tests ---

(deftest open-account-negative-deposit-throws
  (testing "open-account throws on negative initial deposit"
    (is (thrown? clojure.lang.ExceptionInfo
                 (bank/open-account "acc-1" "Alice" -50.0)))))

(deftest deposit-zero-throws
  (testing "deposit throws on zero amount"
    (let [account (bank/open-account "acc-1" "Alice" 100.0)]
      (is (thrown? clojure.lang.ExceptionInfo
                   (bank/deposit account 0.0))))))

(deftest deposit-negative-throws
  (testing "deposit throws on negative amount"
    (let [account (bank/open-account "acc-1" "Alice" 100.0)]
      (is (thrown? clojure.lang.ExceptionInfo
                   (bank/deposit account -10.0))))))

(deftest withdraw-zero-throws
  (testing "withdraw throws on zero amount"
    (let [account (bank/open-account "acc-1" "Alice" 100.0)]
      (is (thrown? clojure.lang.ExceptionInfo
                   (bank/withdraw account 0.0))))))

(deftest withdraw-negative-throws
  (testing "withdraw throws on negative amount"
    (let [account (bank/open-account "acc-1" "Alice" 100.0)]
      (is (thrown? clojure.lang.ExceptionInfo
                   (bank/withdraw account -10.0))))))

(deftest withdraw-exactly-balance-succeeds
  (testing "withdraw succeeds when amount equals balance"
    (let [account (bank/open-account "acc-1" "Alice" 100.0)
          result (bank/withdraw account 100.0)]
      (is (= 0.0 (:balance result))))))

(deftest transfer-negative-throws
  (testing "transfer-to throws on negative amount"
    (let [account (bank/open-account "acc-1" "Alice" 100.0)]
      (is (thrown? clojure.lang.ExceptionInfo
                   (bank/transfer-to account "acc-2" -10.0))))))

(deftest transfer-zero-throws
  (testing "transfer-to throws on zero amount"
    (let [account (bank/open-account "acc-1" "Alice" 100.0)]
      (is (thrown? clojure.lang.ExceptionInfo
                   (bank/transfer-to account "acc-2" 0.0))))))

(deftest transfer-insufficient-funds-throws
  (testing "transfer-to throws when insufficient funds"
    (let [account (bank/open-account "acc-1" "Alice" 50.0)]
      (is (thrown? clojure.lang.ExceptionInfo
                   (bank/transfer-to account "acc-2" 100.0))))))

(deftest receive-transfer-increases-balance-with-description
  (testing "receive-transfer with description"
    (let [account (bank/open-account "acc-2" "Bob" 50.0)
          result (bank/receive-transfer account "acc-1" 75.0 :description "Rent payment")]
      (is (= 125.0 (:balance result))))))

(deftest deposit-with-description
  (testing "deposit with description"
    (let [account (bank/open-account "acc-1" "Alice" 100.0)
          result (bank/deposit account 50.0 :description "Salary")]
      (is (= 150.0 (:balance result)))))

  (testing "deposit without description defaults to empty string"
    (let [account (bank/open-account "acc-1" "Alice" 100.0)
          result (bank/deposit account 50.0)]
      (is (= 150.0 (:balance result))))))

(deftest withdraw-with-description
  (testing "withdraw with description"
    (let [account (bank/open-account "acc-1" "Alice" 100.0)
          result (bank/withdraw account 30.0 :description "Groceries")]
      (is (= 70.0 (:balance result))))))

(deftest transfer-to-with-description
  (testing "transfer-to with description"
    (let [account (bank/open-account "acc-1" "Alice" 200.0)
          result (bank/transfer-to account "acc-2" 75.0 :description "Rent")]
      (is (= 125.0 (:balance result))))))

;; --- Aggregate tests ---

(deftest make-bank-account-defaults
  (testing "make-bank-account returns proper defaults"
    (let [acc (bank/make-bank-account)]
      (is (nil? (:aggregate-id acc)))
      (is (= 0 (:version acc)))
      (is (= 0 (:balance acc)))
      (is (= "" (:account-holder acc)))
      (is (empty? (:uncommitted acc))))))

(deftest replay-events-rebuilds-state
  (testing "replay-events rebuilds aggregate from event list"
    (let [events [{:event-type :account-opened :aggregate-id "acc-1" :account-holder "Alice" :initial-deposit 100.0 :version 1}
                  {:event-type :money-deposited :aggregate-id "acc-1" :amount 50.0 :version 2}
                  {:event-type :money-withdrawn :aggregate-id "acc-1" :amount 20.0 :version 3}]
          result (agg/replay-events (bank/make-bank-account) events)]
      (is (= "acc-1" (:aggregate-id result)))
      (is (= "Alice" (:account-holder result)))
      (is (= 130.0 (:balance result)))
      (is (= 3 (:version result))))))

(deftest aggregate-type-returns-bank-account
  (testing "aggregate-type returns 'bank-account'"
    (let [acc (bank/make-bank-account)]
      (is (= "bank-account" (agg/aggregate-type acc))))))

(deftest serialize-state-returns-correct-keys
  (testing "serialize-state returns only domain keys"
    (let [acc (-> (bank/open-account "acc-1" "Alice" 100.0)
                  (bank/deposit 50.0))
          serialized (agg/serialize-state acc)]
      (is (= "acc-1" (:aggregate-id serialized)))
      (is (= "Alice" (:account-holder serialized)))
      (is (= 150.0 (:balance serialized)))
      (is (nil? (:version serialized)))
      (is (nil? (:uncommitted serialized))))))

(deftest restore-from-snapshot-merges-state
  (testing "restore-from-snapshot restores domain state"
    (let [account (-> (bank/open-account "acc-1" "Alice" 100.0)
                      (bank/deposit 50.0))
          snapshot (agg/create-snapshot account)
          restored (agg/restore-from-snapshot (bank/make-bank-account) snapshot)]
      (is (= "acc-1" (:aggregate-id restored)))
      (is (= "Alice" (:account-holder restored)))
      (is (= 150.0 (:balance restored)))
      (is (= 2 (:version restored)))
      (is (empty? (:uncommitted restored))))))

;; --- Projection tests ---

(deftest summary-projection-handles-all-event-types
  (testing "account summary projection processes all event types"
    (let [summary (bank/create-account-summary-projection)
          events [{:event-type :account-opened :aggregate-id "acc-1" :account-holder "Alice" :initial-deposit 100.0 :timestamp "2024-01-01T00:00:00Z" :sequence-number 1}
                  {:event-type :money-deposited :aggregate-id "acc-1" :amount 50.0 :description "Pay" :timestamp "2024-01-01T00:01:00Z" :sequence-number 2}
                  {:event-type :money-withdrawn :aggregate-id "acc-1" :amount 20.0 :description "Coffee" :timestamp "2024-01-01T00:02:00Z" :sequence-number 3}
                  {:event-type :money-transferred :aggregate-id "acc-1" :amount 30.0 :description "Rent" :target-account-id "acc-2" :timestamp "2024-01-01T00:03:00Z" :sequence-number 4}
                  {:event-type :transfer-received :aggregate-id "acc-1" :amount 25.0 :description "Gift" :source-account-id "acc-3" :timestamp "2024-01-01T00:04:00Z" :sequence-number 5}]]
      (doseq [event events]
        (proj/handle-event summary event))
      (let [alice (get @summary "acc-1")]
        (is (= "Alice" (:account-holder alice)))
        (is (= 125.0 (:balance alice)))
        (is (= 5 (:transaction-count alice)))))))

(deftest transaction-history-projection-handles-all-event-types
  (testing "transaction history projection processes all event types"
    (let [tx-history (bank/create-transaction-history-projection)
          events [{:event-type :account-opened :aggregate-id "acc-1" :account-holder "Alice" :initial-deposit 100.0 :timestamp "2024-01-01T00:00:00Z" :sequence-number 1}
                  {:event-type :money-deposited :aggregate-id "acc-1" :amount 50.0 :description "Pay" :timestamp "2024-01-01T00:01:00Z" :sequence-number 2}
                  {:event-type :money-withdrawn :aggregate-id "acc-1" :amount 20.0 :description "Coffee" :timestamp "2024-01-01T00:02:00Z" :sequence-number 3}
                  {:event-type :money-transferred :aggregate-id "acc-1" :amount 30.0 :description "Rent" :target-account-id "acc-2" :timestamp "2024-01-01T00:03:00Z" :sequence-number 4}
                  {:event-type :transfer-received :aggregate-id "acc-1" :amount 25.0 :description "Gift" :source-account-id "acc-3" :timestamp "2024-01-01T00:04:00Z" :sequence-number 5}]]
      (doseq [event events]
        (proj/handle-event tx-history event))
      (let [history (get @tx-history "acc-1")]
        (is (= 5 (count history)))
        (is (= "Account Opened" (:type (nth history 0))))
        (is (= "Deposit" (:type (nth history 1))))
        (is (= "Withdrawal" (:type (nth history 2))))
        (is (= "Transfer Out" (:type (nth history 3))))
        (is (= "Transfer In" (:type (nth history 4)))))))

(deftest summary-projection-handles-multiple-accounts
  (testing "summary projection tracks multiple accounts independently"
    (let [summary (bank/create-account-summary-projection)]
      (proj/handle-event summary {:event-type :account-opened :aggregate-id "acc-1" :account-holder "Alice" :initial-deposit 100.0 :timestamp "2024-01-01T00:00:00Z" :sequence-number 1})
      (proj/handle-event summary {:event-type :account-opened :aggregate-id "acc-2" :account-holder "Bob" :initial-deposit 50.0 :timestamp "2024-01-01T00:00:00Z" :sequence-number 2})
      (let [alice (get @summary "acc-1")
            bob (get @summary "acc-2")]
        (is (= "Alice" (:account-holder alice)))
        (is (= 100.0 (:balance alice)))
        (is (= "Bob" (:account-holder bob)))
        (is (= 50.0 (:balance bob))))))))