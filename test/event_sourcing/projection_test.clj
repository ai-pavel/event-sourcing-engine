(ns event-sourcing.projection-test
  "Tests for the projection engine: create, register, catch-up, and
   interaction with the account-summary and transaction-history projections."
  (:require [clojure.test :refer [deftest is testing]]
            [event-sourcing.projection :as proj]
            [event-sourcing.sample.bank-account :as bank]))

;; ---------------------------------------------------------------------------
;; create-projection-engine
;; ---------------------------------------------------------------------------

(deftest create-projection-engine-returns-correct-structure
  (let [engine (proj/create-projection-engine)]
    (is (instance? clojure.lang.Atom (:projections engine)))
    (is (instance? clojure.lang.Atom (:last-processed-seq engine)))
    (is (= [] @(:projections engine)))
    (is (= 0 @(:last-processed-seq engine)))))

;; ---------------------------------------------------------------------------
;; register!
;; ---------------------------------------------------------------------------

(deftest register-adds-projection-to-engine
  (let [engine (proj/create-projection-engine)
        p1 (bank/create-account-summary-projection)
        result (proj/register! engine p1)]
    (is (= engine result) "register! returns the engine for chaining")
    (is (= 1 (count @(:projections engine))))))

(deftest register-multiple-projections
  (let [engine (proj/create-projection-engine)
        p1 (bank/create-account-summary-projection)
        p2 (bank/create-transaction-history-projection)
        result (-> engine
                   (proj/register! p1)
                   (proj/register! p2))]
    (is (= engine result))
    (is (= 2 (count @(:projections engine))))))

;; ---------------------------------------------------------------------------
;; catch-up!
;; ---------------------------------------------------------------------------

(deftest catch-up-with-no-events
  (let [engine (-> (proj/create-projection-engine)
                   (proj/register! (bank/create-account-summary-projection)))]
    (proj/catch-up! engine (fn [_] []))
    (is (= 0 @(:last-processed-seq engine)))))

(deftest catch-up-updates-last-processed-seq
  (let [engine (-> (proj/create-projection-engine)
                   (proj/register! (bank/create-account-summary-projection)))
        events [{:event-type :account-opened
                 :aggregate-id "a-1"
                 :account-holder "X"
                 :initial-deposit 100
                 :sequence-number 1}
                {:event-type :money-deposited
                 :aggregate-id "a-1"
                 :amount 50
                 :sequence-number 2}]]
    (proj/catch-up! engine (fn [_] events))
    (is (= 2 @(:last-processed-seq engine)))))

(deftest catch-up-uses-version-when-no-sequence-number
  (let [engine (-> (proj/create-projection-engine)
                   (proj/register! (bank/create-account-summary-projection)))
        events [{:event-type :account-opened
                 :aggregate-id "a-1"
                 :account-holder "X"
                 :initial-deposit 100
                 :version 1}]]
    (proj/catch-up! engine (fn [_] events))
    (is (= 1 @(:last-processed-seq engine)))))

(deftest catch-up-passes-last-processed-seq-to-get-events-fn
  (let [received-arg (atom nil)
        engine (-> (proj/create-projection-engine)
                   (proj/register! (bank/create-account-summary-projection)))]
    ;; Prime the engine to sequence 5
    (reset! (:last-processed-seq engine) 5)
    (proj/catch-up! engine (fn [after-seq]
                             (reset! received-arg after-seq)
                             []))
    (is (= 5 @received-arg))))

(deftest catch-up-dispatches-to-all-registered-projections
  (let [summary (bank/create-account-summary-projection)
        history (bank/create-transaction-history-projection)
        engine (-> (proj/create-projection-engine)
                   (proj/register! summary)
                   (proj/register! history))
        events [{:event-type :account-opened
                 :aggregate-id "a-1"
                 :account-holder "Alice"
                 :initial-deposit 100
                 :timestamp "2024-01-01T00:00:00Z"
                 :sequence-number 1}]]
    (proj/catch-up! engine (fn [_] events))
    ;; Both projections should have processed the event
    (is (= "Alice" (:account-holder (get @summary "a-1"))))
    (is (= 1 (count (get @history "a-1"))))))

(deftest catch-up-incremental
  (testing "Calling catch-up! twice processes events incrementally"
    (let [summary (bank/create-account-summary-projection)
          engine (-> (proj/create-projection-engine)
                     (proj/register! summary))
          batch1 [{:event-type :account-opened
                   :aggregate-id "a-1"
                   :account-holder "X"
                   :initial-deposit 100
                   :sequence-number 1}]
          batch2 [{:event-type :money-deposited
                   :aggregate-id "a-1"
                   :amount 50
                   :sequence-number 2}]]
      (proj/catch-up! engine (fn [_] batch1))
      (is (= 100 (:balance (get @summary "a-1"))))
      (proj/catch-up! engine (fn [_] batch2))
      (is (== 150 (:balance (get @summary "a-1"))))
      (is (= 2 @(:last-processed-seq engine))))))

;; ---------------------------------------------------------------------------
;; Account summary projection -- all event types
;; ---------------------------------------------------------------------------

(deftest account-summary-handles-all-event-types
  (let [summary (bank/create-account-summary-projection)
        engine (-> (proj/create-projection-engine)
                   (proj/register! summary))
        events [{:event-type :account-opened
                 :aggregate-id "a-1"
                 :account-holder "Alice"
                 :initial-deposit 1000
                 :sequence-number 1}
                {:event-type :money-deposited
                 :aggregate-id "a-1"
                 :amount 200
                 :sequence-number 2}
                {:event-type :money-withdrawn
                 :aggregate-id "a-1"
                 :amount 50
                 :sequence-number 3}
                {:event-type :money-transferred
                 :aggregate-id "a-1"
                 :amount 100
                 :sequence-number 4}
                {:event-type :transfer-received
                 :aggregate-id "a-1"
                 :amount 75
                 :sequence-number 5}]]
    (proj/catch-up! engine (fn [_] events))
    (let [s (get @summary "a-1")]
      (is (= "a-1" (:account-id s)))
      (is (= "Alice" (:account-holder s)))
      ;; 1000 + 200 - 50 - 100 + 75 = 1125
      (is (== 1125 (:balance s)))
      (is (= 5 (:transaction-count s))))))

(deftest account-summary-handles-multiple-accounts
  (let [summary (bank/create-account-summary-projection)
        engine (-> (proj/create-projection-engine)
                   (proj/register! summary))
        events [{:event-type :account-opened
                 :aggregate-id "a-1"
                 :account-holder "Alice"
                 :initial-deposit 500
                 :sequence-number 1}
                {:event-type :account-opened
                 :aggregate-id "a-2"
                 :account-holder "Bob"
                 :initial-deposit 300
                 :sequence-number 2}]]
    (proj/catch-up! engine (fn [_] events))
    (is (= "Alice" (:account-holder (get @summary "a-1"))))
    (is (= "Bob" (:account-holder (get @summary "a-2"))))
    (is (= 500 (:balance (get @summary "a-1"))))
    (is (= 300 (:balance (get @summary "a-2"))))))

(deftest account-summary-ignores-unknown-event-type
  (testing "Unknown event types do not crash the projection"
    (let [summary (bank/create-account-summary-projection)
          engine (-> (proj/create-projection-engine)
                     (proj/register! summary))
          events [{:event-type :account-opened
                   :aggregate-id "a-1"
                   :account-holder "X"
                   :initial-deposit 100
                   :sequence-number 1}
                  {:event-type :some-unknown-event
                   :aggregate-id "a-1"
                   :sequence-number 2}]]
      ;; Should not throw
      (proj/catch-up! engine (fn [_] events))
      (is (= 100 (:balance (get @summary "a-1")))))))

;; ---------------------------------------------------------------------------
;; Transaction history projection -- all event types
;; ---------------------------------------------------------------------------

(deftest transaction-history-handles-all-event-types
  (let [history (bank/create-transaction-history-projection)
        engine (-> (proj/create-projection-engine)
                   (proj/register! history))
        events [{:event-type :account-opened
                 :aggregate-id "a-1"
                 :account-holder "Alice"
                 :initial-deposit 1000
                 :timestamp "t1"
                 :sequence-number 1}
                {:event-type :money-deposited
                 :aggregate-id "a-1"
                 :amount 200
                 :description "Salary"
                 :timestamp "t2"
                 :sequence-number 2}
                {:event-type :money-withdrawn
                 :aggregate-id "a-1"
                 :amount 50
                 :description "ATM"
                 :timestamp "t3"
                 :sequence-number 3}
                {:event-type :money-transferred
                 :aggregate-id "a-1"
                 :amount 100
                 :target-account-id "a-2"
                 :description "Rent"
                 :timestamp "t4"
                 :sequence-number 4}
                {:event-type :transfer-received
                 :aggregate-id "a-1"
                 :amount 75
                 :source-account-id "a-3"
                 :description "Refund"
                 :timestamp "t5"
                 :sequence-number 5}]]
    (proj/catch-up! engine (fn [_] events))
    (let [records (get @history "a-1")]
      (is (= 5 (count records)))

      ;; Account opened
      (is (= "Account Opened" (:type (nth records 0))))
      (is (= 1000 (:amount (nth records 0))))

      ;; Deposit
      (is (= "Deposit" (:type (nth records 1))))
      (is (= 200 (:amount (nth records 1))))
      (is (= "Salary" (:description (nth records 1))))

      ;; Withdrawal
      (is (= "Withdrawal" (:type (nth records 2))))
      (is (= -50 (:amount (nth records 2))))
      (is (= "ATM" (:description (nth records 2))))

      ;; Transfer Out
      (is (= "Transfer Out" (:type (nth records 3))))
      (is (= -100 (:amount (nth records 3))))
      (is (clojure.string/includes? (:description (nth records 3)) "a-2"))

      ;; Transfer In
      (is (= "Transfer In" (:type (nth records 4))))
      (is (= 75 (:amount (nth records 4))))
      (is (clojure.string/includes? (:description (nth records 4)) "a-3")))))

(deftest transaction-history-multiple-accounts
  (let [history (bank/create-transaction-history-projection)
        engine (-> (proj/create-projection-engine)
                   (proj/register! history))
        events [{:event-type :account-opened
                 :aggregate-id "a-1"
                 :account-holder "Alice"
                 :initial-deposit 100
                 :timestamp "t1"
                 :sequence-number 1}
                {:event-type :account-opened
                 :aggregate-id "a-2"
                 :account-holder "Bob"
                 :initial-deposit 200
                 :timestamp "t2"
                 :sequence-number 2}]]
    (proj/catch-up! engine (fn [_] events))
    (is (= 1 (count (get @history "a-1"))))
    (is (= 1 (count (get @history "a-2"))))))

(deftest transaction-history-ignores-unknown-event-type
  (let [history (bank/create-transaction-history-projection)
        engine (-> (proj/create-projection-engine)
                   (proj/register! history))
        events [{:event-type :account-opened
                 :aggregate-id "a-1"
                 :account-holder "X"
                 :initial-deposit 100
                 :timestamp "t1"
                 :sequence-number 1}
                {:event-type :unknown-event
                 :aggregate-id "a-1"
                 :sequence-number 2}]]
    ;; Should not throw
    (proj/catch-up! engine (fn [_] events))
    (is (= 1 (count (get @history "a-1"))))))
