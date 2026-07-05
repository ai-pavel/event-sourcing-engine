(ns event-sourcing.bank-account-commands-test
  "Tests for bank account command functions: validation, edge cases,
   default descriptions, protocol fallbacks."
  (:require [clojure.test :refer [deftest is testing]]
            [event-sourcing.aggregate :as agg]
            [event-sourcing.sample.bank-account :as bank]))

;; ---------------------------------------------------------------------------
;; open-account
;; ---------------------------------------------------------------------------

(deftest open-account-with-zero-deposit
  (let [account (bank/open-account "a-1" "X" 0)]
    (is (= 0 (:balance account)))
    (is (= 1 (:version account)))))

(deftest open-account-negative-deposit-throws
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo
        #"Initial deposit cannot be negative"
        (bank/open-account "a-1" "X" -10))))

(deftest open-account-sets-all-fields
  (let [account (bank/open-account "a-1" "Alice" 500.0)]
    (is (= "a-1" (:aggregate-id account)))
    (is (= "Alice" (:account-holder account)))
    (is (= 500.0 (:balance account)))
    (is (= 1 (:version account)))
    (is (= 1 (count (agg/get-uncommitted-events account))))
    (let [event (first (agg/get-uncommitted-events account))]
      (is (= :account-opened (:event-type event)))
      (is (some? (:event-id event)))
      (is (some? (:timestamp event))))))

;; ---------------------------------------------------------------------------
;; deposit
;; ---------------------------------------------------------------------------

(deftest deposit-zero-amount-throws
  (let [account (bank/open-account "a-1" "X" 100)]
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo
          #"Deposit amount must be positive"
          (bank/deposit account 0)))))

(deftest deposit-negative-amount-throws
  (let [account (bank/open-account "a-1" "X" 100)]
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo
          #"Deposit amount must be positive"
          (bank/deposit account -50)))))

(deftest deposit-default-description
  (let [account (-> (bank/open-account "a-1" "X" 100)
                    (bank/deposit 50))
        event (last (agg/get-uncommitted-events account))]
    (is (= "" (:description event)))))

(deftest deposit-custom-description
  (let [account (-> (bank/open-account "a-1" "X" 100)
                    (bank/deposit 50 :description "Bonus"))
        event (last (agg/get-uncommitted-events account))]
    (is (= "Bonus" (:description event)))))

(deftest deposit-multiple-times
  (let [account (-> (bank/open-account "a-1" "X" 100)
                    (bank/deposit 25)
                    (bank/deposit 25)
                    (bank/deposit 50))]
    (is (== 200 (:balance account)))
    (is (= 4 (:version account)))))

;; ---------------------------------------------------------------------------
;; withdraw
;; ---------------------------------------------------------------------------

(deftest withdraw-zero-amount-throws
  (let [account (bank/open-account "a-1" "X" 100)]
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo
          #"Withdrawal amount must be positive"
          (bank/withdraw account 0)))))

(deftest withdraw-negative-amount-throws
  (let [account (bank/open-account "a-1" "X" 100)]
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo
          #"Withdrawal amount must be positive"
          (bank/withdraw account -50)))))

(deftest withdraw-exact-balance
  (let [account (-> (bank/open-account "a-1" "X" 100.0)
                    (bank/withdraw 100.0))]
    (is (= 0.0 (:balance account)))))

(deftest withdraw-exceeds-balance-throws
  (let [account (bank/open-account "a-1" "X" 100.0)]
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo
          #"Insufficient funds"
          (bank/withdraw account 100.01)))))

(deftest withdraw-default-description
  (let [account (-> (bank/open-account "a-1" "X" 100)
                    (bank/withdraw 10))
        event (last (agg/get-uncommitted-events account))]
    (is (= "" (:description event)))))

;; ---------------------------------------------------------------------------
;; transfer-to
;; ---------------------------------------------------------------------------

(deftest transfer-to-zero-amount-throws
  (let [account (bank/open-account "a-1" "X" 100)]
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo
          #"Transfer amount must be positive"
          (bank/transfer-to account "a-2" 0)))))

(deftest transfer-to-negative-amount-throws
  (let [account (bank/open-account "a-1" "X" 100)]
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo
          #"Transfer amount must be positive"
          (bank/transfer-to account "a-2" -10)))))

(deftest transfer-to-insufficient-funds-throws
  (let [account (bank/open-account "a-1" "X" 100)]
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo
          #"Insufficient funds"
          (bank/transfer-to account "a-2" 200)))))

(deftest transfer-to-exact-balance
  (let [account (-> (bank/open-account "a-1" "X" 100.0)
                    (bank/transfer-to "a-2" 100.0))]
    (is (= 0.0 (:balance account)))))

(deftest transfer-to-sets-target-account-id
  (let [account (-> (bank/open-account "a-1" "X" 100.0)
                    (bank/transfer-to "a-2" 50.0 :description "Rent"))
        event (last (agg/get-uncommitted-events account))]
    (is (= "a-2" (:target-account-id event)))
    (is (= :money-transferred (:event-type event)))
    (is (= "Rent" (:description event)))))

(deftest transfer-to-default-description
  (let [account (-> (bank/open-account "a-1" "X" 100.0)
                    (bank/transfer-to "a-2" 50.0))
        event (last (agg/get-uncommitted-events account))]
    (is (= "" (:description event)))))

;; ---------------------------------------------------------------------------
;; receive-transfer
;; ---------------------------------------------------------------------------

(deftest receive-transfer-sets-source-account-id
  (let [account (-> (bank/open-account "a-2" "Bob" 100.0)
                    (bank/receive-transfer "a-1" 50.0 :description "Gift"))
        event (last (agg/get-uncommitted-events account))]
    (is (= "a-1" (:source-account-id event)))
    (is (= :transfer-received (:event-type event)))
    (is (= "Gift" (:description event)))))

(deftest receive-transfer-default-description
  (let [account (-> (bank/open-account "a-2" "Bob" 100.0)
                    (bank/receive-transfer "a-1" 50.0))
        event (last (agg/get-uncommitted-events account))]
    (is (= "" (:description event)))))

(deftest receive-transfer-increases-balance
  (let [account (-> (bank/open-account "a-2" "Bob" 100.0)
                    (bank/receive-transfer "a-1" 75.0))]
    (is (= 175.0 (:balance account)))))

;; ---------------------------------------------------------------------------
;; Protocol fallbacks (maps without metadata functions)
;; ---------------------------------------------------------------------------

(deftest apply-event-fallback-returns-same-state
  (let [plain-map {:aggregate-id "x" :version 1 :uncommitted []}
        result (agg/apply-event plain-map {:event-type :anything})]
    (is (= plain-map result))))

(deftest aggregate-type-fallback-returns-unknown
  (let [plain-map {}]
    (is (= "unknown" (agg/aggregate-type plain-map)))))

(deftest serialize-state-fallback-dissocs-version-uncommitted
  (let [plain-map {:aggregate-id "x" :version 1 :uncommitted [] :balance 50}
        result (agg/serialize-state plain-map)]
    (is (not (contains? result :version)))
    (is (not (contains? result :uncommitted)))
    (is (= "x" (:aggregate-id result)))
    (is (= 50 (:balance result)))))

(deftest restore-state-fallback-merges
  (let [plain-map {:aggregate-id "x"}
        result (agg/restore-state plain-map {:balance 100 :name "Y"})]
    (is (= 100 (:balance result)))
    (is (= "Y" (:name result)))
    (is (= "x" (:aggregate-id result)))))

;; ---------------------------------------------------------------------------
;; Unknown event type on bank account
;; ---------------------------------------------------------------------------

(deftest unknown-event-type-ignored
  (let [agg (bank/make-bank-account)
        started (agg/raise-event agg {:event-type :account-opened
                                       :aggregate-id "a-1"
                                       :account-holder "X"
                                       :initial-deposit 100})
        result (agg/raise-event started {:event-type :unknown-event
                                          :some-data 42})]
    ;; Version still increments, balance unchanged
    (is (= 2 (:version result)))
    (is (= 100 (:balance result)))))

;; ---------------------------------------------------------------------------
;; make-bank-account
;; ---------------------------------------------------------------------------

(deftest make-bank-account-initial-state
  (let [agg (bank/make-bank-account)]
    (is (nil? (:aggregate-id agg)))
    (is (= 0 (:version agg)))
    (is (= [] (:uncommitted agg)))
    (is (= "" (:account-holder agg)))
    (is (= 0 (:balance agg)))))

;; ---------------------------------------------------------------------------
;; Full lifecycle through commands
;; ---------------------------------------------------------------------------

(deftest full-lifecycle-scenario
  (testing "Open, deposit, withdraw, transfer, receive -- full chain"
    (let [alice (bank/open-account "a-1" "Alice" 1000.0)
          alice (bank/deposit alice 500.0 :description "Salary")
          alice (bank/withdraw alice 200.0 :description "Groceries")
          alice (bank/transfer-to alice "a-2" 300.0 :description "Rent")

          bob (bank/open-account "a-2" "Bob" 0)
          bob (bank/receive-transfer bob "a-1" 300.0 :description "Rent")]
      (is (= 1000.0 (:balance alice)))
      (is (= 4 (:version alice)))
      (is (= 300.0 (:balance bob)))
      (is (= 2 (:version bob))))))
