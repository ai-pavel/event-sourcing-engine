(ns event-sourcing.aggregate
  "Protocols and functions for event-sourced aggregate roots.

   An aggregate is represented as a plain map with at least:
     :aggregate-id  - unique identifier
     :version       - current version (incremented per event)
     :uncommitted   - vector of events not yet persisted

   Domain-specific state lives alongside these keys in the same map.")

(defprotocol AggregateRoot
  "Protocol for event-sourced aggregates. Implementations define how
   events mutate state and how state is serialized for snapshots."
  (apply-event [aggregate event]
    "Applies a domain event to the aggregate, returning the new state map.
     Must be a pure function -- no side effects.")
  (aggregate-type [aggregate]
    "Returns a string identifier for this aggregate type.")
  (serialize-state [aggregate]
    "Serializes the aggregate's domain state to a map suitable for JSON storage.")
  (restore-state [aggregate state-map]
    "Restores domain state from a deserialized snapshot map."))

(defn raise-event
  "Raises a new domain event on the aggregate. Increments the version,
   applies the event to mutate state, and appends to uncommitted events."
  [aggregate event]
  (let [new-version (inc (:version aggregate 0))
        versioned-event (assoc event :version new-version)
        updated (apply-event aggregate versioned-event)]
    (-> updated
        (assoc :version new-version)
        (update :uncommitted (fnil conj []) versioned-event))))

(defn replay-event
  "Replays a previously persisted event to rebuild aggregate state.
   Updates the version but does not add to uncommitted events."
  [aggregate event]
  (-> (apply-event aggregate event)
      (assoc :version (:version event))))

(defn replay-events
  "Replays a sequence of events onto an aggregate using reduce."
  [aggregate events]
  (reduce replay-event aggregate events))

(defn get-uncommitted-events
  "Returns the vector of uncommitted events."
  [aggregate]
  (:uncommitted aggregate []))

(defn clear-uncommitted-events
  "Clears the uncommitted events list after persistence."
  [aggregate]
  (assoc aggregate :uncommitted []))

(defn create-snapshot
  "Creates a snapshot map from the aggregate's current state."
  [aggregate]
  {:aggregate-id   (:aggregate-id aggregate)
   :version        (:version aggregate)
   :aggregate-type (aggregate-type aggregate)
   :state          (serialize-state aggregate)
   :timestamp      (java.time.Instant/now)})

(defn restore-from-snapshot
  "Restores an aggregate from a snapshot map."
  [aggregate snapshot]
  (-> (restore-state aggregate (:state snapshot))
      (assoc :version (:version snapshot))
      (assoc :uncommitted [])))
