(ns event-sourcing.sample.bank-account
  "Sample domain: a bank account aggregate with deposit, withdraw, and transfer events.

   The aggregate state is a plain map:
     {:aggregate-id   \"ACC-001\"
      :version        3
      :uncommitted    [...]
      :account-holder \"Alice\"
      :balance        1000.00}"
  (:require [event-sourcing.aggregate :as agg]
            [event-sourcing.projection :as proj]))

;; ---------------------------------------------------------------------------
;; Aggregate implementation
;; ---------------------------------------------------------------------------

(defn- apply-bank-event
  "Pure function that applies a bank account event to the aggregate state."
  [state event]
  (case (:event-type event)
    :account-opened
    (-> state
        (assoc :aggregate-id (:aggregate-id event))
        (assoc :account-holder (:account-holder event))
        (assoc :balance (:initial-deposit event)))

    :money-deposited
    (update state :balance + (:amount event))

    :money-withdrawn
    (update state :balance - (:amount event))

    :money-transferred
    (update state :balance - (:amount event))

    :transfer-received
    (update state :balance + (:amount event))

    ;; Unknown event types are ignored
    state))

(def empty-bank-account
  "Initial state for a new bank account aggregate."
  (reify agg/AggregateRoot
    (apply-event [this event] (apply-bank-event this event))
    (aggregate-type [_] "bank-account")
    (serialize-state [this]
      (select-keys this [:aggregate-id :account-holder :balance]))
    (restore-state [this state-map]
      (merge this state-map))))

(defn make-bank-account
  "Creates a new empty bank account map that implements AggregateRoot."
  []
  (with-meta {:aggregate-id nil
              :version 0
              :uncommitted []
              :account-holder ""
              :balance 0}
    {`agg/apply-event (fn [state event] (apply-bank-event state event))
     `agg/aggregate-type (fn [_] "bank-account")
     `agg/serialize-state (fn [state] (select-keys state [:aggregate-id :account-holder :balance]))
     `agg/restore-state (fn [state state-map] (merge state state-map))}))

;; We extend the protocol to plain maps via metadata isn't straightforward,
;; so we extend it to IPersistentMap directly.
(extend-type clojure.lang.IPersistentMap
  agg/AggregateRoot
  (apply-event [this event]
    (if-let [f (get (meta this) `agg/apply-event)]
      (let [result (f this event)]
        (with-meta result (meta this)))
      this))
  (aggregate-type [this]
    (if-let [f (get (meta this) `agg/aggregate-type)]
      (f this)
      "unknown"))
  (serialize-state [this]
    (if-let [f (get (meta this) `agg/serialize-state)]
      (f this)
      (dissoc this :version :uncommitted)))
  (restore-state [this state-map]
    (if-let [f (get (meta this) `agg/restore-state)]
      (let [result (f this state-map)]
        (with-meta result (meta this)))
      (merge this state-map))))

;; ---------------------------------------------------------------------------
;; Command functions (raise events with validation)
;; ---------------------------------------------------------------------------

(defn open-account
  "Opens a new bank account with an initial deposit."
  [account-id account-holder initial-deposit]
  (when (neg? initial-deposit)
    (throw (ex-info "Initial deposit cannot be negative."
                    {:initial-deposit initial-deposit})))
  (agg/raise-event (make-bank-account)
                   {:event-type     :account-opened
                    :aggregate-id   account-id
                    :account-holder account-holder
                    :initial-deposit initial-deposit
                    :event-id       (str (java.util.UUID/randomUUID))
                    :timestamp      (str (java.time.Instant/now))}))

(defn deposit
  "Deposits money into the account."
  [account amount & {:keys [description] :or {description ""}}]
  (when (<= amount 0)
    (throw (ex-info "Deposit amount must be positive." {:amount amount})))
  (agg/raise-event account
                   {:event-type   :money-deposited
                    :aggregate-id (:aggregate-id account)
                    :amount       amount
                    :description  description
                    :event-id     (str (java.util.UUID/randomUUID))
                    :timestamp    (str (java.time.Instant/now))}))

(defn withdraw
  "Withdraws money from the account."
  [account amount & {:keys [description] :or {description ""}}]
  (when (<= amount 0)
    (throw (ex-info "Withdrawal amount must be positive." {:amount amount})))
  (when (> amount (:balance account 0))
    (throw (ex-info (format "Insufficient funds. Balance: %.2f, Requested: %.2f"
                            (double (:balance account 0)) (double amount))
                    {:balance (:balance account) :amount amount})))
  (agg/raise-event account
                   {:event-type   :money-withdrawn
                    :aggregate-id (:aggregate-id account)
                    :amount       amount
                    :description  description
                    :event-id     (str (java.util.UUID/randomUUID))
                    :timestamp    (str (java.time.Instant/now))}))

(defn transfer-to
  "Transfers money to another account (outbound side)."
  [account target-account-id amount & {:keys [description] :or {description ""}}]
  (when (<= amount 0)
    (throw (ex-info "Transfer amount must be positive." {:amount amount})))
  (when (> amount (:balance account 0))
    (throw (ex-info (format "Insufficient funds. Balance: %.2f, Requested: %.2f"
                            (double (:balance account 0)) (double amount))
                    {:balance (:balance account) :amount amount})))
  (agg/raise-event account
                   {:event-type        :money-transferred
                    :aggregate-id      (:aggregate-id account)
                    :target-account-id target-account-id
                    :amount            amount
                    :description       description
                    :event-id          (str (java.util.UUID/randomUUID))
                    :timestamp         (str (java.time.Instant/now))}))

(defn receive-transfer
  "Receives a transfer from another account (inbound side)."
  [account source-account-id amount & {:keys [description] :or {description ""}}]
  (agg/raise-event account
                   {:event-type        :transfer-received
                    :aggregate-id      (:aggregate-id account)
                    :source-account-id source-account-id
                    :amount            amount
                    :description       description
                    :event-id          (str (java.util.UUID/randomUUID))
                    :timestamp         (str (java.time.Instant/now))}))

;; ---------------------------------------------------------------------------
;; Projections
;; ---------------------------------------------------------------------------

(defn create-account-summary-projection
  "Creates a projection that maintains account summaries in an atom.
   The atom holds a map of {account-id -> {:account-id, :account-holder, :balance, :transaction-count}}."
  []
  (let [state (atom {})]
    (reify
      proj/Projection
      (handle-event [_ event]
        (case (:event-type event)
          :account-opened
          (swap! state assoc (:aggregate-id event)
                 {:account-id       (:aggregate-id event)
                  :account-holder   (:account-holder event)
                  :balance          (:initial-deposit event)
                  :transaction-count 1})

          :money-deposited
          (swap! state update-in [(:aggregate-id event)]
                 (fn [acc]
                   (-> acc
                       (update :balance + (:amount event))
                       (update :transaction-count inc))))

          :money-withdrawn
          (swap! state update-in [(:aggregate-id event)]
                 (fn [acc]
                   (-> acc
                       (update :balance - (:amount event))
                       (update :transaction-count inc))))

          :money-transferred
          (swap! state update-in [(:aggregate-id event)]
                 (fn [acc]
                   (-> acc
                       (update :balance - (:amount event))
                       (update :transaction-count inc))))

          :transfer-received
          (swap! state update-in [(:aggregate-id event)]
                 (fn [acc]
                   (-> acc
                       (update :balance + (:amount event))
                       (update :transaction-count inc))))

          nil))

      clojure.lang.IDeref
      (deref [_] @state))))

(defn create-transaction-history-projection
  "Creates a projection that maintains per-account transaction history in an atom.
   The atom holds a map of {account-id -> [{:timestamp, :type, :amount, :description}]}."
  []
  (let [state (atom {})]
    (reify
      proj/Projection
      (handle-event [_ event]
        (let [agg-id (:aggregate-id event)]
          (case (:event-type event)
            :account-opened
            (swap! state update agg-id (fnil conj [])
                   {:timestamp   (:timestamp event)
                    :type        "Account Opened"
                    :amount      (:initial-deposit event)
                    :description (str "Account opened for " (:account-holder event))})

            :money-deposited
            (swap! state update agg-id (fnil conj [])
                   {:timestamp   (:timestamp event)
                    :type        "Deposit"
                    :amount      (:amount event)
                    :description (:description event)})

            :money-withdrawn
            (swap! state update agg-id (fnil conj [])
                   {:timestamp   (:timestamp event)
                    :type        "Withdrawal"
                    :amount      (- (:amount event))
                    :description (:description event)})

            :money-transferred
            (swap! state update agg-id (fnil conj [])
                   {:timestamp   (:timestamp event)
                    :type        "Transfer Out"
                    :amount      (- (:amount event))
                    :description (str "To " (:target-account-id event) ": " (:description event))})

            :transfer-received
            (swap! state update agg-id (fnil conj [])
                   {:timestamp   (:timestamp event)
                    :type        "Transfer In"
                    :amount      (:amount event)
                    :description (str "From " (:source-account-id event) ": " (:description event))})

            nil)))

      clojure.lang.IDeref
      (deref [_] @state))))
