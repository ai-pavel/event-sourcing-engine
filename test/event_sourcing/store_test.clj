(ns event-sourcing.store-test
  (:require [clojure.test :refer [deftest is testing]]
            [event-sourcing.aggregate :as agg]
            [event-sourcing.store :as store]
            [event-sourcing.sample.bank-account :as bank]))

(defn- temp-db []
  (let [db-path (str "test_store_" (System/nanoTime) ".db")
        ds (store/create-datasource db-path)]
    (store/initialize! ds)
    [ds db-path]))

(defn- cleanup [db-path]
  (.delete (java.io.File. db-path)))

(deftest append-events-with-correct-version
  (testing "append-events! stores events and they can be retrieved"
    (let [[ds db-path] (temp-db)]
      (try
        (let [account (-> (bank/open-account "acc-1" "Alice" 100.0)
                          (bank/deposit 50.0))
              events (agg/get-uncommitted-events account)
              _ (store/append-events! ds "acc-1" events 0)
              loaded (store/get-events ds "acc-1")]
          (is (= 2 (count loaded)))
          (is (= :account-opened (:event-type (first loaded))))
          (is (= :money-deposited (:event-type (second loaded)))))
        (finally
          (cleanup db-path))))))

(deftest append-events-concurrency-conflict-throws
  (testing "append-events! throws on version mismatch"
    (let [[ds db-path] (temp-db)]
      (try
        (let [account (bank/open-account "acc-1" "Alice" 100.0)
              events (agg/get-uncommitted-events account)
              _ (store/append-events! ds "acc-1" events 0)]
          ;; Try to append with wrong expected version
          (is (thrown? clojure.lang.ExceptionInfo
                       (store/append-events! ds "acc-1" events 5))))
        (finally
          (cleanup db-path))))))

(deftest get-events-after-version
  (testing "get-events returns only events after the given version"
    (let [[ds db-path] (temp-db)]
      (try
        (let [account (-> (bank/open-account "acc-1" "Alice" 100.0)
                          (bank/deposit 50.0)
                          (bank/withdraw 20.0))
              events (agg/get-uncommitted-events account)
              _ (store/append-events! ds "acc-1" events 0)
              ;; Get events after version 1
              later-events (store/get-events ds "acc-1" 1)]
          (is (= 2 (count later-events)))
          (is (= :money-deposited (:event-type (first later-events))))
          (is (= :money-withdrawn (:event-type (second later-events)))))
        (finally
          (cleanup db-path))))))

(deftest get-events-for-nonexistent-aggregate
  (testing "get-events returns empty for unknown aggregate"
    (let [[ds db-path] (temp-db)]
      (try
        (is (empty? (store/get-events ds "unknown-aggregate")))
        (finally
          (cleanup db-path))))))

(deftest get-all-events-across-aggregates
  (testing "get-all-events returns events from all aggregates"
    (let [[ds db-path] (temp-db)]
      (try
        (let [alice (bank/open-account "acc-1" "Alice" 100.0)
              bob (bank/open-account "acc-2" "Bob" 50.0)
              _ (store/save-aggregate! ds alice 0)
              _ (store/save-aggregate! ds bob 0)
              all (store/get-all-events ds)]
          (is (= 2 (count all))))
        (finally
          (cleanup db-path))))))

(deftest get-all-events-after-sequence
  (testing "get-all-events with after-sequence returns only new events"
    (let [[ds db-path] (temp-db)]
      (try
        (let [alice (bank/open-account "acc-1" "Alice" 100.0)
              _ (store/save-aggregate! ds alice 0)
              first-seq (:sequence-number (first (store/get-all-events ds)))
              bob (bank/open-account "acc-2" "Bob" 50.0)
              _ (store/save-aggregate! ds bob 0)
              new-events (store/get-all-events ds first-seq)]
          (is (= 1 (count new-events)))
          (is (= "acc-2" (:aggregate-id (first new-events)))))
        (finally
          (cleanup db-path))))))

(deftest save-and-load-snapshot
  (testing "save-snapshot! and get-latest-snapshot round-trip"
    (let [[ds db-path] (temp-db)]
      (try
        (let [account (-> (bank/open-account "acc-1" "Alice" 100.0)
                          (bank/deposit 50.0))
              snapshot (agg/create-snapshot account)
              _ (store/save-snapshot! ds snapshot)
              loaded (store/get-latest-snapshot ds "acc-1")]
          (is (some? loaded))
          (is (= "acc-1" (:aggregate-id loaded)))
          (is (= 2 (:version loaded)))
          (is (= "bank-account" (:aggregate-type loaded)))
          (is (= "Alice" (:account-holder (:state loaded))))
          (is (= 150.0 (:balance (:state loaded)))))
        (finally
          (cleanup db-path))))))

(deftest get-latest-snapshot-returns-nil-when-none
  (testing "get-latest-snapshot returns nil when no snapshot exists"
    (let [[ds db-path] (temp-db)]
      (try
        (is (nil? (store/get-latest-snapshot ds "unknown")))
        (finally
          (cleanup db-path))))))

(deftest save-snapshot-upserts
  (testing "save-snapshot! overwrites existing snapshot"
    (let [[ds db-path] (temp-db)]
      (try
        (let [account1 (-> (bank/open-account "acc-1" "Alice" 100.0)
                           (bank/deposit 50.0))
              _ (store/save-snapshot! ds (agg/create-snapshot account1))
              account2 (-> account1 (bank/deposit 100.0))
              _ (store/save-snapshot! ds (agg/create-snapshot account2))
              loaded (store/get-latest-snapshot ds "acc-1")]
          (is (= 3 (:version loaded)))
          (is (= 250.0 (:balance (:state loaded)))))
        (finally
          (cleanup db-path))))))

(deftest save-aggregate-with-snapshot-interval
  (testing "save-aggregate! creates snapshot when interval is reached"
    (let [[ds db-path] (temp-db)]
      (try
        (let [account (-> (bank/open-account "acc-1" "Alice" 100.0)
                          (bank/deposit 50.0)
                          (bank/withdraw 20.0)
                          (bank/deposit 10.0)
                          (bank/deposit 10.0))]
          ;; Version is 5, snapshot interval = 5
          (store/save-aggregate! ds account 5)
          (let [snapshot (store/get-latest-snapshot ds "acc-1")]
            (is (some? snapshot))
            (is (= 5 (:version snapshot)))
            (is (= 150.0 (:balance (:state snapshot))))))
        (finally
          (cleanup db-path))))))

(deftest save-aggregate-no-snapshot-with-zero-interval
  (testing "save-aggregate! does not create snapshot when interval is 0"
    (let [[ds db-path] (temp-db)]
      (try
        (let [account (-> (bank/open-account "acc-1" "Alice" 100.0)
                          (bank/deposit 50.0))]
          (store/save-aggregate! ds account 0)
          (is (nil? (store/get-latest-snapshot ds "acc-1"))))
        (finally
          (cleanup db-path))))))

(deftest load-aggregate-from-snapshot
  (testing "load-aggregate restores from snapshot and replays remaining events"
    (let [[ds db-path] (temp-db)]
      (try
        (let [account (-> (bank/open-account "acc-1" "Alice" 100.0)
                          (bank/deposit 50.0))]
          ;; Save with snapshot at version 2
          (store/save-aggregate! ds account 2)
          ;; Now add more events
          (let [account2 (-> (store/load-aggregate ds (bank/make-bank-account) "acc-1")
                            (bank/withdraw 20.0))]
            (store/save-aggregate! ds account2 0)
            (let [reloaded (store/load-aggregate ds (bank/make-bank-account) "acc-1")]
              (is (= 130.0 (:balance reloaded)))
              (is (= 3 (:version reloaded))))))
        (finally
          (cleanup db-path))))))

(deftest save-aggregate-clears-uncommitted
  (testing "save-aggregate! returns aggregate with cleared uncommitted events"
    (let [[ds db-path] (temp-db)]
      (try
        (let [account (-> (bank/open-account "acc-1" "Alice" 100.0)
                          (bank/deposit 50.0))
              saved (store/save-aggregate! ds account 0)]
          (is (empty? (agg/get-uncommitted-events saved))))
        (finally
          (cleanup db-path))))))