(ns event-sourcing.core-test
  (:require [clojure.test :refer [deftest is testing]]
            [event-sourcing.core :as core]
            [event-sourcing.store :as store]
            [event-sourcing.projection :as proj]
            [event-sourcing.sample.bank-account :as bank])
  (:import (java.io File)))

(defn- capture-stdout
  "Captures everything printed to *out* while executing f. Returns the printed
  string."
  [f]
  (let [sw (java.io.StringWriter.)]
    (binding [*out* sw]
      (f))
    (str sw)))

(deftest -main-runs-demo-and-cleans-up
  (testing "core/-main runs the bank account demo end-to-end"
    (let [output (capture-stdout #(core/-main))]
      (is (.contains output "Event Sourcing Engine"))
      (is (.contains output "Opening accounts..."))
      (is (.contains output "Alice: Alice Johnson, Balance: $1000.00"))
      (is (.contains output "Bob:   Bob Smith, Balance: $500.00"))
      (is (.contains output "Performing transactions..."))
      (is (.contains output "Alice balance after transactions"))
      (is (.contains output "Bob balance after transactions"))
      (is (.contains output "Reloading aggregates from event store..."))
      (is (.contains output "Alice (reloaded):"))
      (is (.contains output "Bob (reloaded):"))
      (is (.contains output "Building projections from event store..."))
      (is (.contains output "Account Summaries"))
      (is (.contains output "Transaction History"))
      (is (.contains output "Creating additional transactions to trigger snapshot..."))
      (is (.contains output "Snapshot found for Alice"))
      (is (.contains output "Alice (final reload):"))
      (is (.contains output "Done.")))))

(deftest demo-database-is-cleaned-up
  (testing "core/-main deletes its temporary database file on completion"
    ;; Collect db paths created during the run by patching create-datasource
    ;; is overkill; instead verify no lingering sample_bank_*.db files remain
    ;; after -main returns (modulo any that existed before).
    (let [before (set (map #(.getName ^File %)
                           (.listFiles (File. "."))))
          _ (core/-main)
          after (set (map #(.getName ^File %)
                          (.listFiles (File. "."))))
          new-files (remove before after)]
      (is (empty? (filter #(.startsWith ^String % "sample_bank_") new-files))
          "no sample_bank_*.db files should be left behind"))))