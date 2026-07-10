(ns event-sourcing.projection-test
  (:require [clojure.test :refer [deftest is testing]]
            [event-sourcing.projection :as proj]
            [event-sourcing.sample.bank-account :as bank]))

(deftest projection-engine-creation
  (testing "create-projection-engine returns map with atoms"
    (let [engine (proj/create-projection-engine)]
      (is (= [] @(engine :projections)))
      (is (= 0 @(engine :last-processed-seq)))))

(deftest register-projection
  (testing "register! adds projection to engine"
    (let [engine (proj/create-projection-engine)
          summary (bank/create-account-summary-projection)
          updated (proj/register! engine summary)]
      (is (= 1 (count @(updated :projections))))
      (is (identical? engine updated) "register! returns engine for chaining"))))

(deftest catch-up-processes-all-events
  (testing "catch-up! processes all events through registered projections"
    (let [engine (proj/create-projection-engine)
          summary (bank/create-account-summary-projection)
          tx-history (bank/create-transaction-history-projection)
          engine (-> engine (proj/register! summary) (proj/register! tx-history))
          events [{:event-type :account-opened :aggregate-id "acc-1" :account-holder "Alice" :initial-deposit 100.0 :timestamp "2024-01-01T00:00:00Z" :sequence-number 1}
                  {:event-type :money-deposited :aggregate-id "acc-1" :amount 50.0 :description "Pay" :timestamp "2024-01-01T00:01:00Z" :sequence-number 2}]
          _ (proj/catch-up! engine (fn [_] events))
          alice-summary (get @summary "acc-1")
          alice-history (get @tx-history "acc-1")]
      (is (= "Alice" (:account-holder alice-summary)))
      (is (= 150.0 (:balance alice-summary)))
      (is (= 2 (:transaction-count alice-summary)))
      (is (= 2 (count alice-history))))))

(deftest catch-up-tracks-sequence-number
  (testing "catch-up! updates last-processed-seq"
    (let [engine (proj/create-projection-engine)
          summary (bank/create-account-summary-projection)
          engine (proj/register! engine summary)
          events [{:event-type :account-opened :aggregate-id "acc-1" :account-holder "Alice" :initial-deposit 100.0 :timestamp "2024-01-01T00:00:00Z" :sequence-number 5}]
          _ (proj/catch-up! engine (fn [_] events))]
      (is (= 5 @(engine :last-processed-seq)))))

(deftest catch-up-with-no-projections
  (testing "catch-up! works with no registered projections"
    (let [engine (proj/create-projection-engine)
          events [{:event-type :account-opened :aggregate-id "acc-1" :account-holder "Alice" :initial-deposit 100.0 :timestamp "2024-01-01T00:00:00Z" :sequence-number 1}]
          _ (proj/catch-up! engine (fn [_] events))]
      (is (= 1 @(engine :last-processed-seq)))))

(deftest catch-up-with-empty-events
  (testing "catch-up! handles empty event list"
    (let [engine (proj/create-projection-engine)
          _ (proj/catch-up! engine (fn [_] []))]
      (is (= 0 @(engine :last-processed-seq))))))

(deftest catch-up-incremental
  (testing "catch-up! only processes events after last-processed-seq"
    (let [engine (proj/create-projection-engine)
          summary (bank/create-account-summary-projection)
          engine (proj/register! engine summary)
          all-events [{:event-type :account-opened :aggregate-id "acc-1" :account-holder "Alice" :initial-deposit 100.0 :timestamp "2024-01-01T00:00:00Z" :sequence-number 1}
                      {:event-type :money-deposited :aggregate-id "acc-1" :amount 50.0 :description "Pay" :timestamp "2024-01-01T00:01:00Z" :sequence-number 2}
                      {:event-type :money-withdrawn :aggregate-id "acc-1" :amount 20.0 :description "Coffee" :timestamp "2024-01-01T00:02:00Z" :sequence-number 3}]
          ;; First catch-up processes first 2 events
          _ (proj/catch-up! engine (fn [_] (take 2 all-events)))]
      (is (= 2 @(engine :last-processed-seq)))
      (is (= 150.0 (:balance (get @summary "acc-1"))))
      ;; Second catch-up processes remaining events
      (proj/catch-up! engine (fn [after-seq] (filter (fn [e] (> (:sequence-number e) after-seq)) all-events)))
      (is (= 130.0 (:balance (get @summary "acc-1"))))))))))