(ns event-sourcing.store-test
  "Tests for the event store: datasource creation, schema initialization,
   event append/get, snapshots, concurrency, aggregate load/save."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [event-sourcing.store :as store]
            [event-sourcing.aggregate :as agg]
            [event-sourcing.sample.bank-account :as bank]))

;; ---------------------------------------------------------------------------
;; Helpers
;; ---------------------------------------------------------------------------

(defn- temp-db []
  (str "test_store_" (System/nanoTime) ".db"))

(defmacro with-temp-store [[ds-sym] & body]
  `(let [db-path# (temp-db)
         ~ds-sym (store/create-datasource db-path#)]
     (try
       (store/initialize! ~ds-sym)
       ~@body
       (finally
         (.delete (java.io.File. db-path#))))))

;; ---------------------------------------------------------------------------
;; create-datasource / initialize!
;; ---------------------------------------------------------------------------

(deftest create-datasource-returns-non-nil
  (let [ds (store/create-datasource (temp-db))]
    (is (some? ds))))

(deftest initialize-creates-tables-idempotent
  (with-temp-store [ds]
    ;; Calling initialize! twice should not throw
    (store/initialize! ds)
    (is true)))

;; ---------------------------------------------------------------------------
;; append-events! / get-events
;; ---------------------------------------------------------------------------

(deftest append-and-get-events
  (with-temp-store [ds]
    (let [events [{:event-type :account-opened
                   :aggregate-id "a-1"
                   :account-holder "Alice"
                   :initial-deposit 100
                   :version 1
                   :event-id "e-1"
                   :timestamp "2024-01-01T00:00:00Z"}]]
      (store/append-events! ds "a-1" events 0)
      (let [loaded (store/get-events ds "a-1")]
        (is (= 1 (count loaded)))
        (is (= :account-opened (:event-type (first loaded))))
        (is (= 1 (:version (first loaded))))
        (is (= "a-1" (:aggregate-id (first loaded))))
        (is (some? (:sequence-number (first loaded))))))))

(deftest get-events-after-version
  (with-temp-store [ds]
    (let [events [{:event-type :account-opened
                   :aggregate-id "a-1"
                   :account-holder "X"
                   :initial-deposit 100
                   :version 1
                   :event-id "e-1"
                   :timestamp "t1"}
                  {:event-type :money-deposited
                   :aggregate-id "a-1"
                   :amount 50
                   :version 2
                   :event-id "e-2"
                   :timestamp "t2"}
                  {:event-type :money-withdrawn
                   :aggregate-id "a-1"
                   :amount 10
                   :version 3
                   :event-id "e-3"
                   :timestamp "t3"}]]
      (store/append-events! ds "a-1" events 0)

      (testing "after-version 0 returns all"
        (is (= 3 (count (store/get-events ds "a-1" 0)))))

      (testing "after-version 1 returns 2 events"
        (let [result (store/get-events ds "a-1" 1)]
          (is (= 2 (count result)))
          (is (= 2 (:version (first result))))))

      (testing "after-version 3 returns empty"
        (is (empty? (store/get-events ds "a-1" 3)))))))

(deftest get-events-nonexistent-aggregate
  (with-temp-store [ds]
    (is (empty? (store/get-events ds "nonexistent")))))

;; ---------------------------------------------------------------------------
;; Concurrency conflict
;; ---------------------------------------------------------------------------

(deftest append-events-concurrency-conflict
  (with-temp-store [ds]
    (let [events [{:event-type :account-opened
                   :aggregate-id "a-1"
                   :account-holder "X"
                   :initial-deposit 100
                   :version 1
                   :event-id "e-1"
                   :timestamp "t1"}]]
      (store/append-events! ds "a-1" events 0)

      (testing "Wrong expected version throws"
        (is (thrown-with-msg?
              clojure.lang.ExceptionInfo
              #"Concurrency conflict"
              (store/append-events! ds "a-1"
                [{:event-type :money-deposited
                  :aggregate-id "a-1"
                  :amount 50
                  :version 2
                  :event-id "e-2"
                  :timestamp "t2"}]
                0))))  ;; expected 0, but actual is 1

      (testing "Correct expected version succeeds"
        (store/append-events! ds "a-1"
          [{:event-type :money-deposited
            :aggregate-id "a-1"
            :amount 50
            :version 2
            :event-id "e-2"
            :timestamp "t2"}]
          1)
        (is (= 2 (count (store/get-events ds "a-1"))))))))

;; ---------------------------------------------------------------------------
;; get-all-events
;; ---------------------------------------------------------------------------

(deftest get-all-events-across-aggregates
  (with-temp-store [ds]
    (store/append-events! ds "a-1"
      [{:event-type :account-opened :aggregate-id "a-1"
        :account-holder "Alice" :initial-deposit 100
        :version 1 :event-id "e-1" :timestamp "t1"}]
      0)
    (store/append-events! ds "a-2"
      [{:event-type :account-opened :aggregate-id "a-2"
        :account-holder "Bob" :initial-deposit 200
        :version 1 :event-id "e-2" :timestamp "t2"}]
      0)

    (testing "get-all-events returns events from all aggregates"
      (let [all-events (store/get-all-events ds)]
        (is (= 2 (count all-events)))
        (is (= #{"a-1" "a-2"} (set (map :aggregate-id all-events))))))

    (testing "get-all-events with after-sequence"
      (let [all-events (store/get-all-events ds)
            first-seq (:sequence-number (first all-events))
            after (store/get-all-events ds first-seq)]
        (is (= 1 (count after)))))))

(deftest get-all-events-empty-store
  (with-temp-store [ds]
    (is (empty? (store/get-all-events ds)))
    (is (empty? (store/get-all-events ds 0)))))

;; ---------------------------------------------------------------------------
;; Snapshots
;; ---------------------------------------------------------------------------

(deftest save-and-get-snapshot
  (with-temp-store [ds]
    (let [snapshot {:aggregate-id "a-1"
                    :version 5
                    :aggregate-type "bank-account"
                    :state {:aggregate-id "a-1" :account-holder "Alice" :balance 500.0}
                    :timestamp (java.time.Instant/now)}]
      (store/save-snapshot! ds snapshot)
      (let [loaded (store/get-latest-snapshot ds "a-1")]
        (is (some? loaded))
        (is (= "a-1" (:aggregate-id loaded)))
        (is (= 5 (:version loaded)))
        (is (= "bank-account" (:aggregate-type loaded)))
        (is (= "Alice" (:account-holder (:state loaded))))
        (is (= 500.0 (:balance (:state loaded))))))))

(deftest save-snapshot-upserts
  (with-temp-store [ds]
    (let [snap1 {:aggregate-id "a-1" :version 3 :aggregate-type "bank-account"
                 :state {:balance 300} :timestamp (java.time.Instant/now)}
          snap2 {:aggregate-id "a-1" :version 6 :aggregate-type "bank-account"
                 :state {:balance 600} :timestamp (java.time.Instant/now)}]
      (store/save-snapshot! ds snap1)
      (store/save-snapshot! ds snap2)
      (let [loaded (store/get-latest-snapshot ds "a-1")]
        (is (= 6 (:version loaded)))
        (is (= 600 (:balance (:state loaded))))))))

(deftest get-latest-snapshot-returns-nil-when-none
  (with-temp-store [ds]
    (is (nil? (store/get-latest-snapshot ds "nonexistent")))))

;; ---------------------------------------------------------------------------
;; load-aggregate
;; ---------------------------------------------------------------------------

(deftest load-aggregate-from-events
  (with-temp-store [ds]
    (let [account (-> (bank/open-account "a-1" "Alice" 100.0)
                      (bank/deposit 50.0))
          _ (store/save-aggregate! ds account 0)
          loaded (store/load-aggregate ds (bank/make-bank-account) "a-1")]
      (is (= "a-1" (:aggregate-id loaded)))
      (is (= "Alice" (:account-holder loaded)))
      (is (= 150.0 (:balance loaded)))
      (is (= 2 (:version loaded)))
      (is (empty? (:uncommitted loaded))))))

(deftest load-aggregate-with-snapshot
  (with-temp-store [ds]
    ;; Create an account with 5 events (snapshot-interval = 5)
    (let [account (-> (bank/open-account "a-1" "Alice" 100.0)
                      (bank/deposit 10.0)
                      (bank/deposit 20.0)
                      (bank/deposit 30.0)
                      (bank/deposit 40.0))
          _ (store/save-aggregate! ds account 5)  ;; snapshot at version 5
          ;; Verify snapshot was created
          snapshot (store/get-latest-snapshot ds "a-1")]
      (is (some? snapshot))
      (is (= 5 (:version snapshot)))

      ;; Add more events after the snapshot
      (let [loaded (store/load-aggregate ds (bank/make-bank-account) "a-1")
            updated (bank/deposit loaded 50.0)
            _ (store/save-aggregate! ds updated 0)
            ;; Reload -- should use snapshot + replay only event 6
            reloaded (store/load-aggregate ds (bank/make-bank-account) "a-1")]
        (is (= 6 (:version reloaded)))
        (is (= 250.0 (:balance reloaded)))))))

(deftest load-aggregate-nonexistent-returns-empty
  (with-temp-store [ds]
    (let [loaded (store/load-aggregate ds (bank/make-bank-account) "nonexistent")]
      (is (= 0 (:version loaded)))
      (is (= "nonexistent" (:aggregate-id loaded))))))

;; ---------------------------------------------------------------------------
;; save-aggregate!
;; ---------------------------------------------------------------------------

(deftest save-aggregate-with-no-uncommitted-events
  (with-temp-store [ds]
    (let [account (agg/clear-uncommitted-events
                    (bank/open-account "a-1" "Alice" 100.0))
          result (store/save-aggregate! ds account 0)]
      ;; Should not throw, no events appended
      (is (empty? (:uncommitted result)))
      ;; Nothing in store
      (is (empty? (store/get-events ds "a-1"))))))

(deftest save-aggregate-default-snapshot-interval
  (testing "Default snapshot interval is 50"
    (with-temp-store [ds]
      ;; Build an aggregate with exactly 50 events (1 open + 49 deposits)
      (let [account (bank/open-account "a-1" "Alice" 100.0)
            account (reduce (fn [acc i]
                              (bank/deposit acc 1.0 :description (str "d" i)))
                            account (range 49))
            _ (store/save-aggregate! ds account)] ;; uses default interval 50
        ;; Snapshot should be created at version 50
        (let [snapshot (store/get-latest-snapshot ds "a-1")]
          (is (some? snapshot))
          (is (= 50 (:version snapshot))))))))

(deftest save-aggregate-snapshot-not-created-below-interval
  (with-temp-store [ds]
    (let [account (-> (bank/open-account "a-1" "Alice" 100.0)
                      (bank/deposit 50.0))  ;; version 2
          _ (store/save-aggregate! ds account 5)] ;; interval 5, version 2 not multiple
      (is (nil? (store/get-latest-snapshot ds "a-1"))))))

(deftest save-aggregate-snapshot-disabled
  (with-temp-store [ds]
    (let [account (-> (bank/open-account "a-1" "Alice" 100.0)
                      (bank/deposit 50.0)
                      (bank/deposit 50.0)
                      (bank/deposit 50.0)
                      (bank/deposit 50.0))  ;; version 5
          _ (store/save-aggregate! ds account 0)] ;; interval 0 = disabled
      (is (nil? (store/get-latest-snapshot ds "a-1"))))))
