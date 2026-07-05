(ns event-sourcing.aggregate-test
  "Tests for the aggregate namespace: raise-event, replay-event(s),
   get/clear uncommitted events, create-snapshot, restore-from-snapshot."
  (:require [clojure.test :refer [deftest is testing]]
            [event-sourcing.aggregate :as agg]
            [event-sourcing.sample.bank-account :as bank]))

;; ---------------------------------------------------------------------------
;; raise-event
;; ---------------------------------------------------------------------------

(deftest raise-event-increments-version
  (testing "raise-event increments from zero"
    (let [agg (bank/make-bank-account)
          result (agg/raise-event agg {:event-type :account-opened
                                       :aggregate-id "a-1"
                                       :account-holder "X"
                                       :initial-deposit 0})]
      (is (= 1 (:version result)))))

  (testing "raise-event increments from existing version"
    (let [agg (assoc (bank/make-bank-account) :version 5)
          result (agg/raise-event agg {:event-type :money-deposited
                                       :aggregate-id "a-1"
                                       :amount 10})]
      (is (= 6 (:version result))))))

(deftest raise-event-appends-to-uncommitted
  (let [agg (bank/make-bank-account)
        result (agg/raise-event agg {:event-type :account-opened
                                     :aggregate-id "a-1"
                                     :account-holder "X"
                                     :initial-deposit 0})]
    (is (= 1 (count (:uncommitted result))))
    (is (= :account-opened (:event-type (first (:uncommitted result)))))
    (is (= 1 (:version (first (:uncommitted result)))))))

(deftest raise-event-applies-event-to-state
  (let [agg (bank/make-bank-account)
        result (agg/raise-event agg {:event-type :account-opened
                                     :aggregate-id "a-1"
                                     :account-holder "Alice"
                                     :initial-deposit 500})]
    (is (= "Alice" (:account-holder result)))
    (is (= 500 (:balance result)))
    (is (= "a-1" (:aggregate-id result)))))

(deftest raise-event-with-nil-version-defaults-to-zero
  (testing "When :version key is missing, defaults to 0 and increments to 1"
    (let [agg (dissoc (bank/make-bank-account) :version)
          result (agg/raise-event agg {:event-type :account-opened
                                       :aggregate-id "a-1"
                                       :account-holder "X"
                                       :initial-deposit 0})]
      (is (= 1 (:version result))))))

(deftest raise-event-multiple-events-sequential
  (let [agg (bank/make-bank-account)
        r1 (agg/raise-event agg {:event-type :account-opened
                                  :aggregate-id "a-1"
                                  :account-holder "X"
                                  :initial-deposit 100})
        r2 (agg/raise-event r1 {:event-type :money-deposited
                                 :aggregate-id "a-1"
                                 :amount 50})
        r3 (agg/raise-event r2 {:event-type :money-withdrawn
                                 :aggregate-id "a-1"
                                 :amount 30})]
    (is (= 3 (:version r3)))
    (is (== 120 (:balance r3)))
    (is (= 3 (count (:uncommitted r3))))))

;; ---------------------------------------------------------------------------
;; replay-event / replay-events
;; ---------------------------------------------------------------------------

(deftest replay-event-updates-version-without-uncommitted
  (let [agg (bank/make-bank-account)
        event {:event-type :account-opened
               :aggregate-id "a-1"
               :account-holder "Alice"
               :initial-deposit 100
               :version 1}
        result (agg/replay-event agg event)]
    (is (= 1 (:version result)))
    (is (= "Alice" (:account-holder result)))
    (is (= 100 (:balance result)))
    (is (empty? (:uncommitted result)))))

(deftest replay-events-replays-full-stream
  (let [agg (bank/make-bank-account)
        events [{:event-type :account-opened
                 :aggregate-id "a-1"
                 :account-holder "Bob"
                 :initial-deposit 200
                 :version 1}
                {:event-type :money-deposited
                 :aggregate-id "a-1"
                 :amount 100
                 :version 2}
                {:event-type :money-withdrawn
                 :aggregate-id "a-1"
                 :amount 50
                 :version 3}]
        result (agg/replay-events agg events)]
    (is (= 3 (:version result)))
    (is (== 250 (:balance result)))
    (is (= "Bob" (:account-holder result)))
    (is (empty? (:uncommitted result)))))

(deftest replay-events-with-empty-list-returns-same-aggregate
  (let [agg (bank/make-bank-account)
        result (agg/replay-events agg [])]
    (is (= agg result))))

;; ---------------------------------------------------------------------------
;; get-uncommitted-events / clear-uncommitted-events
;; ---------------------------------------------------------------------------

(deftest get-uncommitted-events-returns-empty-vector-when-nil
  (let [agg (dissoc (bank/make-bank-account) :uncommitted)]
    (is (= [] (agg/get-uncommitted-events agg)))))

(deftest get-uncommitted-events-returns-existing-events
  (let [agg (assoc (bank/make-bank-account) :uncommitted [{:event-type :test}])]
    (is (= [{:event-type :test}] (agg/get-uncommitted-events agg)))))

(deftest clear-uncommitted-events-empties-the-list
  (let [agg (assoc (bank/make-bank-account) :uncommitted [{:x 1} {:x 2}])
        cleared (agg/clear-uncommitted-events agg)]
    (is (= [] (:uncommitted cleared)))))

;; ---------------------------------------------------------------------------
;; create-snapshot / restore-from-snapshot
;; ---------------------------------------------------------------------------

(deftest create-snapshot-structure
  (let [account (-> (bank/open-account "acc-1" "Alice" 100)
                    (bank/deposit 50))
        snapshot (agg/create-snapshot account)]
    (is (= "acc-1" (:aggregate-id snapshot)))
    (is (= 2 (:version snapshot)))
    (is (= "bank-account" (:aggregate-type snapshot)))
    (is (some? (:timestamp snapshot)))
    (is (map? (:state snapshot)))
    (is (= "Alice" (:account-holder (:state snapshot))))
    (is (== 150 (:balance (:state snapshot))))))

(deftest restore-from-snapshot-restores-correctly
  (let [snapshot {:aggregate-id "acc-1"
                  :version 5
                  :aggregate-type "bank-account"
                  :state {:aggregate-id "acc-1"
                          :account-holder "Carol"
                          :balance 999.0}
                  :timestamp "2024-01-01T00:00:00Z"}
        restored (agg/restore-from-snapshot (bank/make-bank-account) snapshot)]
    (is (= 5 (:version restored)))
    (is (= [] (:uncommitted restored)))
    (is (= "Carol" (:account-holder restored)))
    (is (= 999.0 (:balance restored)))))

(deftest restore-and-continue-raising-events
  (testing "After restoring from snapshot, new events can be raised"
    (let [snapshot {:aggregate-id "acc-1"
                    :version 3
                    :aggregate-type "bank-account"
                    :state {:aggregate-id "acc-1"
                            :account-holder "Eve"
                            :balance 200.0}
                    :timestamp "2024-01-01T00:00:00Z"}
          restored (agg/restore-from-snapshot (bank/make-bank-account) snapshot)
          with-deposit (agg/raise-event restored {:event-type :money-deposited
                                                   :aggregate-id "acc-1"
                                                   :amount 50})]
      (is (= 4 (:version with-deposit)))
      (is (= 250.0 (:balance with-deposit)))
      (is (= 1 (count (:uncommitted with-deposit)))))))

;; ---------------------------------------------------------------------------
;; aggregate-type
;; ---------------------------------------------------------------------------

(deftest aggregate-type-returns-bank-account
  (let [agg (bank/make-bank-account)]
    (is (= "bank-account" (agg/aggregate-type agg)))))

;; ---------------------------------------------------------------------------
;; serialize-state
;; ---------------------------------------------------------------------------

(deftest serialize-state-returns-domain-keys-only
  (let [account (-> (bank/open-account "acc-1" "Alice" 100)
                    (bank/deposit 50))
        serialized (agg/serialize-state account)]
    (is (= #{"aggregate-id" "account-holder" "balance"}
           (set (map name (keys serialized)))))
    (is (not (contains? serialized :version)))
    (is (not (contains? serialized :uncommitted)))))
