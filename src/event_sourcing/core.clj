(ns event-sourcing.core
  "Main entry point demonstrating the event sourcing framework
   with a bank account lifecycle."
  (:require [event-sourcing.store :as store]
            [event-sourcing.projection :as proj]
            [event-sourcing.sample.bank-account :as bank])
  (:gen-class))

(defn -main
  "Runs the bank account event sourcing demo.

   The demo uses a fresh, unique database file per run and deletes it on
   completion, so repeated `lein run` invocations always start from a clean
   slate (rather than crashing with a concurrency conflict when re-opening
   accounts that already exist in a lingering sample_bank.db)."
  [& _args]
  (let [db-path (str "sample_bank_" (System/nanoTime) ".db")
        ds (store/create-datasource db-path)
        snapshot-interval 5]
   (try

    ;; Initialize schema
    (store/initialize! ds)

    (println "=== Event Sourcing Engine - Bank Account Sample ===\n")

    ;; Create accounts
    (println "Opening accounts...")
    (let [alice (bank/open-account "ACC-001" "Alice Johnson" 1000.0)
          alice (store/save-aggregate! ds alice snapshot-interval)

          bob (bank/open-account "ACC-002" "Bob Smith" 500.0)
          bob (store/save-aggregate! ds bob snapshot-interval)]

      (printf "  Alice: %s, Balance: $%.2f%n" (:account-holder alice) (:balance alice))
      (printf "  Bob:   %s, Balance: $%.2f%n" (:account-holder bob) (:balance bob))

      ;; Perform transactions
      (println "\nPerforming transactions...")
      (let [alice (store/load-aggregate ds (bank/make-bank-account) "ACC-001")
            alice (-> alice
                      (bank/deposit 250.0 :description "Paycheck")
                      (bank/withdraw 100.0 :description "Groceries"))
            alice (store/save-aggregate! ds alice snapshot-interval)

            bob (store/load-aggregate ds (bank/make-bank-account) "ACC-002")
            bob (bank/deposit bob 300.0 :description "Freelance payment")
            bob (store/save-aggregate! ds bob snapshot-interval)]

        (let [alice (store/load-aggregate ds (bank/make-bank-account) "ACC-001")
              alice (bank/transfer-to alice "ACC-002" 200.0 :description "Rent payment")
              alice (store/save-aggregate! ds alice snapshot-interval)

              bob (store/load-aggregate ds (bank/make-bank-account) "ACC-002")
              bob (bank/receive-transfer bob "ACC-001" 200.0 :description "Rent payment")
              bob (store/save-aggregate! ds bob snapshot-interval)]

          (printf "  Alice balance after transactions: $%.2f%n" (:balance alice))
          (printf "  Bob balance after transactions:   $%.2f%n" (:balance bob))

          ;; Reload from event store
          (println "\nReloading aggregates from event store...")
          (let [reloaded-alice (store/load-aggregate ds (bank/make-bank-account) "ACC-001")
                reloaded-bob (store/load-aggregate ds (bank/make-bank-account) "ACC-002")]

            (printf "  Alice (reloaded): %s, Balance: $%.2f, Version: %d%n"
                    (:account-holder reloaded-alice) (:balance reloaded-alice) (:version reloaded-alice))
            (printf "  Bob (reloaded):   %s, Balance: $%.2f, Version: %d%n"
                    (:account-holder reloaded-bob) (:balance reloaded-bob) (:version reloaded-bob))

            ;; Build projections
            (println "\nBuilding projections from event store...")
            (let [account-summary (bank/create-account-summary-projection)
                  tx-history (bank/create-transaction-history-projection)
                  engine (-> (proj/create-projection-engine)
                             (proj/register! account-summary)
                             (proj/register! tx-history))]

              (proj/catch-up! engine (fn [after-seq] (store/get-all-events ds after-seq)))

              ;; Display read models
              (println "\n--- Account Summaries ---")
              (doseq [[_ summary] (sort-by first @account-summary)]
                (printf "  [%s] %s | Balance: $%.2f | Transactions: %d%n"
                        (:account-id summary)
                        (:account-holder summary)
                        (:balance summary)
                        (:transaction-count summary)))

              (println "\n--- Alice's Transaction History ---")
              (doseq [record (get @tx-history "ACC-001")]
                (printf "  %s | %-15s | %12.2f | %s%n"
                        (:timestamp record)
                        (:type record)
                        (double (:amount record))
                        (:description record)))

              (println "\n--- Bob's Transaction History ---")
              (doseq [record (get @tx-history "ACC-002")]
                (printf "  %s | %-15s | %12.2f | %s%n"
                        (:timestamp record)
                        (:type record)
                        (double (:amount record))
                        (:description record)))

              ;; Demonstrate snapshot
              (println "\nCreating additional transactions to trigger snapshot...")
              (let [alice-again (store/load-aggregate ds (bank/make-bank-account) "ACC-001")
                    alice-again (reduce (fn [acc i]
                                          (bank/deposit acc 10.0 :description (str "Small deposit #" (inc i))))
                                        alice-again (range 1))
                    _ (store/save-aggregate! ds alice-again snapshot-interval)]

                (if-let [snapshot (store/get-latest-snapshot ds "ACC-001")]
                  (printf "  Snapshot found for Alice at version %d%n" (:version snapshot))
                  (println "  No snapshot yet (will be created at next interval boundary)."))

                ;; Final reload to verify snapshot path
                 (let [final-alice (store/load-aggregate ds (bank/make-bank-account) "ACC-001")]
                   (printf "  Alice (final reload): Balance: $%.2f, Version: %d%n"
                           (:balance final-alice) (:version final-alice)))

                (println "\nDone.")))))))
   (finally
     ;; Clean up the demo database so the next run starts fresh.
     (.delete (java.io.File. db-path))))))
