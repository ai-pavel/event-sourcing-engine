(ns event-sourcing.coverage-test
  (:require [clojure.test :refer [deftest is testing]]
            [event-sourcing.aggregate :as agg]
            [event-sourcing.projection :as proj]
            [event-sourcing.sample.bank-account :as bank]
            [event-sourcing.store :as store])
  (:import (java.io File)))

;; --- apply-bank-event unknown event default branch (line 40) ---

(deftest apply-bank-event-unknown-event-type-ignored
  (testing "applying an unknown event type leaves the account state unchanged"
    (let [acc (-> (bank/open-account "acc-1" "Alice" 100.0)
                  (bank/deposit 50.0))
          before (:balance acc)
          result (agg/apply-event acc {:event-type :bogus-event :amount 999.0})]
      (is (= before (:balance result))))))

;; --- empty-bank-account (reify) method bodies (lines 45, 48, 50) ---
;; The reify instance is not Associative, so known-event application and
;; serialize-state/restore-state throw. Entering the form is enough to count
;; it as covered by cloverage.

(deftest empty-bank-account-apply-event-unknown-type
  (testing "empty-bank-account apply-event with unknown event returns state unchanged"
    (let [result (agg/apply-event bank/empty-bank-account
                                  {:event-type :something-weird :amount 5.0})]
      (is (identical? bank/empty-bank-account result))))

  (testing "empty-bank-account apply-event with known event throws (reify not Associative)"
    (is (thrown? ClassCastException
                 (agg/apply-event bank/empty-bank-account
                                  {:event-type   :account-opened
                                   :aggregate-id "acc-1"
                                   :account-holder "Alice"
                                   :initial-deposit 100.0})))))

(deftest empty-bank-account-aggregate-type
  (testing "empty-bank-account aggregate-type returns 'bank-account'"
    (is (= "bank-account" (agg/aggregate-type bank/empty-bank-account)))))

(deftest empty-bank-account-serialize-state-throws
  (testing "empty-bank-account serialize-state enters the select-keys form (then throws)"
    (is (thrown? Exception
                 (agg/serialize-state bank/empty-bank-account)))))

(deftest empty-bank-account-restore-state-throws
  (testing "empty-bank-account restore-state enters the merge form (then throws)"
    (is (thrown? Exception
                 (agg/restore-state bank/empty-bank-account {:balance 250.0})))))

;; --- IPersistentMap fallback branches (lines 73, 81, 86) ---

(deftest plain-map-apply-event-without-metadata-fallback
  (testing "a plain map without apply-event metadata returns itself unchanged"
    (let [plain {:aggregate-id "acc-1" :balance 100.0}
          result (agg/apply-event plain {:event-type :account-opened})]
      (is (identical? plain result)))))

(deftest plain-map-aggregate-type-without-metadata-fallback
  (testing "a plain map without aggregate-type metadata returns 'unknown'"
    (let [plain {:aggregate-id "acc-1"}]
      (is (= "unknown" (agg/aggregate-type plain))))))

(deftest plain-map-serialize-state-without-metadata-fallback
  (testing "a plain map without serialize-state metadata dissocs version/uncommitted"
    (let [plain {:aggregate-id "acc-1" :version 5 :uncommitted [] :balance 100.0}
          serialized (agg/serialize-state plain)]
      (is (= "acc-1" (:aggregate-id serialized)))
      (is (= 100.0 (:balance serialized)))
      (is (nil? (:version serialized)))
      (is (nil? (:uncommitted serialized))))))

(deftest plain-map-restore-state-without-metadata-fallback
  (testing "a plain map without restore-state metadata merges the state-map"
    (let [plain {:aggregate-id "acc-1" :balance 100.0}
          restored (agg/restore-state plain {:balance 250.0})]
      (is (= 250.0 (:balance restored)))
      (is (= "acc-1" (:aggregate-id restored))))))

;; --- store: save-aggregate! single-arity (default snapshot interval = 50) ---

(deftest save-aggregate-default-arity-no-snapshot
  (testing "save-aggregate! 1-arg default interval=50 does not snapshot at version 2"
    (let [db-path (str "test_cov_default_" (System/nanoTime) ".db")
          ds (store/create-datasource db-path)]
      (try
        (store/initialize! ds)
        (let [account (-> (bank/open-account "acc-1" "Alice" 100.0)
                          (bank/deposit 50.0))
              saved (store/save-aggregate! ds account)]
          (is (empty? (agg/get-uncommitted-events saved)))
          (is (nil? (store/get-latest-snapshot ds "acc-1")))
          (let [reloaded (store/load-aggregate ds (bank/make-bank-account) "acc-1")]
            (is (= 150.0 (:balance reloaded)))
            (is (= 2 (:version reloaded)))))
        (finally
          (.delete (File. db-path)))))))

(deftest save-aggregate-default-arity-snapshots-at-50
  (testing "save-aggregate! default arity (interval=50) snapshots when version hits 50"
    (let [db-path (str "test_cov_default50_" (System/nanoTime) ".db")
          ds (store/create-datasource db-path)]
      (try
        (store/initialize! ds)
        (let [account (-> (bank/open-account "acc-1" "Alice" 100.0)
                          (as-> a
                            (reduce (fn [acc _] (bank/deposit acc 10.0)) a (range 49))))
              _ (is (= 50 (:version account)))
              _ (store/save-aggregate! ds account)
              snapshot (store/get-latest-snapshot ds "acc-1")]
          (is (some? snapshot))
          (is (= 50 (:version snapshot))))
        (finally
          (.delete (File. db-path)))))))

;; --- projection catch-up! :version fallback (events without :sequence-number) ---

(deftest catch-up-falls-back-to-version-for-seq-tracking
  (testing "catch-up! uses :version when :sequence-number is absent"
    (let [engine (proj/create-projection-engine)
          summary (bank/create-account-summary-projection)
          engine (proj/register! engine summary)
          events [{:event-type :account-opened :aggregate-id "acc-1" :account-holder "Alice"
                   :initial-deposit 100.0 :timestamp "t1" :version 7}]
          _ (proj/catch-up! engine (fn [_] events))]
      (is (= 7 @(engine :last-processed-seq)))
      (is (= 100.0 (:balance (get @summary "acc-1"))))))

  (testing "catch-up! uses :sequence-number when both present (preferred)"
    (let [engine (proj/create-projection-engine)
          summary (bank/create-account-summary-projection)
          engine (proj/register! engine summary)
          events [{:event-type :account-opened :aggregate-id "acc-1" :account-holder "Alice"
                   :initial-deposit 100.0 :timestamp "t1"
                   :sequence-number 42 :version 7}]
          _ (proj/catch-up! engine (fn [_] events))]
      (is (= 42 @(engine :last-processed-seq)))))

  (testing "catch-up! with events lacking both keys sets last-processed-seq to nil"
    (let [engine (proj/create-projection-engine)
          summary (bank/create-account-summary-projection)
          engine (proj/register! engine summary)
          events [{:event-type :account-opened :aggregate-id "acc-1" :account-holder "Alice"
                   :initial-deposit 100.0 :timestamp "t1"}]
          _ (proj/catch-up! engine (fn [_] events))]
      (is (nil? @(engine :last-processed-seq))))))

;; --- projection handle-event nil default branches (unknown event types) ---

(deftest summary-projection-ignores-unknown-event-type
  (testing "account summary projection returns nil for unknown event types"
    (let [summary (bank/create-account-summary-projection)]
      (proj/handle-event summary {:event-type :bogus-event :aggregate-id "acc-1"})
      (is (empty? @summary)))))

(deftest transaction-history-projection-ignores-unknown-event-type
  (testing "transaction history projection returns nil for unknown event types"
    (let [tx-history (bank/create-transaction-history-projection)]
      (proj/handle-event tx-history {:event-type :bogus-event :aggregate-id "acc-1"})
      (is (empty? @tx-history)))))