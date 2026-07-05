(ns event-sourcing.store
  "SQLite-backed event store using next.jdbc.

   Supports appending events with optimistic concurrency, loading event streams,
   managing snapshots, and retrieving all events for projection catch-up."
  (:require [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [clojure.data.json :as json]
            [event-sourcing.aggregate :as agg]))

;; ---------------------------------------------------------------------------
;; Schema initialization
;; ---------------------------------------------------------------------------

(def ^:private create-tables-sql
  ["CREATE TABLE IF NOT EXISTS events (
      sequence_number INTEGER PRIMARY KEY AUTOINCREMENT,
      aggregate_id    TEXT    NOT NULL,
      version         INTEGER NOT NULL,
      event_type      TEXT    NOT NULL,
      payload         TEXT    NOT NULL,
      timestamp       TEXT    NOT NULL,
      event_id        TEXT    NOT NULL,
      UNIQUE(aggregate_id, version)
    )"
   "CREATE INDEX IF NOT EXISTS ix_events_aggregate_id ON events(aggregate_id)"
   "CREATE TABLE IF NOT EXISTS snapshots (
      aggregate_id   TEXT    NOT NULL PRIMARY KEY,
      version        INTEGER NOT NULL,
      aggregate_type TEXT    NOT NULL,
      state          TEXT    NOT NULL,
      timestamp      TEXT    NOT NULL
    )"])

;; ---------------------------------------------------------------------------
;; EventStore abstraction
;; ---------------------------------------------------------------------------

(defprotocol EventStore
  "Storage backend for events and snapshots. Implementations may be backed by
   SQLite (durable) or an in-memory atom (fast, isolated tests/demos)."
  (-append-events! [store aggregate-id events expected-version]
    "Appends events with optimistic concurrency on expected-version.")
  (-get-events [store aggregate-id after-version]
    "Returns events for one aggregate with version > after-version.")
  (-get-all-events [store after-sequence limit]
    "Returns events across all aggregates with sequence > after-sequence,
     optionally bounded by limit (nil = unbounded), ordered by sequence.")
  (-save-snapshot! [store snapshot]
    "Upserts a snapshot keyed by aggregate-id.")
  (-get-latest-snapshot [store aggregate-id]
    "Returns the latest snapshot for an aggregate, or nil."))

(defn event-store?
  "True if x implements the EventStore protocol."
  [x]
  (satisfies? EventStore x))

(defn create-datasource
  "Creates a next.jdbc datasource for the given SQLite database path."
  [db-path]
  (jdbc/get-datasource {:dbtype "sqlite" :dbname db-path}))

(defn initialize!
  "Creates the required database tables if they do not already exist."
  [ds]
  (jdbc/with-transaction [tx ds]
    (doseq [sql create-tables-sql]
      (jdbc/execute! tx [sql]))))

;; ---------------------------------------------------------------------------
;; Event persistence
;; ---------------------------------------------------------------------------

(defn- sqlite-append-events!
  "SQLite implementation of append-events! against a raw datasource."
  [ds aggregate-id events expected-version]
  (jdbc/with-transaction [tx ds]
    (let [result (jdbc/execute-one! tx
                   ["SELECT MAX(version) AS max_version FROM events WHERE aggregate_id = ?"
                    aggregate-id]
                   {:builder-fn rs/as-unqualified-lower-maps})
          current-version (or (:max_version result) 0)]
      (when (not= current-version expected-version)
        (throw (ex-info "Concurrency conflict"
                        {:aggregate-id     aggregate-id
                         :expected-version expected-version
                         :actual-version   current-version})))
      (doseq [event events]
        (jdbc/execute! tx
          ["INSERT INTO events (aggregate_id, version, event_type, payload, timestamp, event_id)
            VALUES (?, ?, ?, ?, ?, ?)"
           aggregate-id
           (:version event)
           (name (:event-type event))
           (json/write-str (dissoc event :event-type :version))
           (str (or (:timestamp event) (java.time.Instant/now)))
           (str (or (:event-id event) (java.util.UUID/randomUUID)))])))))

(defn- row->event
  "Converts a database row map to a domain event map."
  [row]
  (let [payload (json/read-str (:payload row) :key-fn keyword)]
    (merge payload
           {:event-type (keyword (:event_type row))
            :version    (:version row)
            :event-id   (:event_id row)
            :timestamp  (:timestamp row)
            :aggregate-id (:aggregate_id row)
            :sequence-number (:sequence_number row)})))

(defn- sqlite-get-events
  "SQLite implementation of get-events against a raw datasource."
  [ds aggregate-id after-version]
  (let [rows (jdbc/execute! ds
               ["SELECT sequence_number, aggregate_id, version, event_type, payload, timestamp, event_id
                 FROM events
                 WHERE aggregate_id = ? AND version > ?
                 ORDER BY version"
                aggregate-id after-version]
               {:builder-fn rs/as-unqualified-lower-maps})]
    (mapv row->event rows)))

(defn- sqlite-get-all-events
  "SQLite implementation of get-all-events against a raw datasource."
  [ds after-sequence limit]
  (let [rows (if limit
               (jdbc/execute! ds
                 ["SELECT sequence_number, aggregate_id, version, event_type, payload, timestamp, event_id
                   FROM events
                   WHERE sequence_number > ?
                   ORDER BY sequence_number
                   LIMIT ?"
                  after-sequence limit]
                 {:builder-fn rs/as-unqualified-lower-maps})
               (jdbc/execute! ds
                 ["SELECT sequence_number, aggregate_id, version, event_type, payload, timestamp, event_id
                   FROM events
                   WHERE sequence_number > ?
                   ORDER BY sequence_number"
                  after-sequence]
                 {:builder-fn rs/as-unqualified-lower-maps}))]
    (mapv row->event rows)))

;; ---------------------------------------------------------------------------
;; Snapshots
;; ---------------------------------------------------------------------------

(defn- sqlite-save-snapshot!
  "SQLite implementation of save-snapshot! against a raw datasource."
  [ds snapshot]
  (jdbc/execute! ds
    ["INSERT INTO snapshots (aggregate_id, version, aggregate_type, state, timestamp)
      VALUES (?, ?, ?, ?, ?)
      ON CONFLICT(aggregate_id) DO UPDATE SET
        version = excluded.version,
        aggregate_type = excluded.aggregate_type,
        state = excluded.state,
        timestamp = excluded.timestamp"
     (:aggregate-id snapshot)
     (:version snapshot)
     (:aggregate-type snapshot)
     (json/write-str (:state snapshot))
     (str (:timestamp snapshot))]))

(defn- sqlite-get-latest-snapshot
  "SQLite implementation of get-latest-snapshot against a raw datasource."
  [ds aggregate-id]
  (when-let [row (jdbc/execute-one! ds
                   ["SELECT aggregate_id, version, aggregate_type, state, timestamp
                     FROM snapshots
                     WHERE aggregate_id = ?"
                    aggregate-id]
                   {:builder-fn rs/as-unqualified-lower-maps})]
    {:aggregate-id   (:aggregate_id row)
     :version        (:version row)
     :aggregate-type (:aggregate_type row)
     :state          (json/read-str (:state row) :key-fn keyword)
     :timestamp      (:timestamp row)}))

;; ---------------------------------------------------------------------------
;; SQLite EventStore implementation
;; ---------------------------------------------------------------------------

(defrecord SqliteEventStore [ds]
  EventStore
  (-append-events! [_ aggregate-id events expected-version]
    (sqlite-append-events! ds aggregate-id events expected-version))
  (-get-events [_ aggregate-id after-version]
    (sqlite-get-events ds aggregate-id after-version))
  (-get-all-events [_ after-sequence limit]
    (sqlite-get-all-events ds after-sequence limit))
  (-save-snapshot! [_ snapshot]
    (sqlite-save-snapshot! ds snapshot))
  (-get-latest-snapshot [_ aggregate-id]
    (sqlite-get-latest-snapshot ds aggregate-id)))

(defn create-sqlite-store
  "Creates a durable SQLite-backed EventStore for the given database path
   and initializes its schema."
  [db-path]
  (let [ds (create-datasource db-path)]
    (initialize! ds)
    (->SqliteEventStore ds)))

;; ---------------------------------------------------------------------------
;; In-memory EventStore implementation (atom-backed, for tests/demos)
;; ---------------------------------------------------------------------------

(defrecord InMemoryEventStore [state]
  ;; state is an atom of {:events [event...] :seq n :snapshots {agg-id snapshot}}
  EventStore
  (-append-events! [_ aggregate-id new-events expected-version]
    (swap! state
           (fn [{:keys [events seq] :as s}]
             (let [current-version (->> events
                                        (filter #(= aggregate-id (:aggregate-id %)))
                                        (map :version)
                                        (reduce max 0))]
               (when (not= current-version expected-version)
                 (throw (ex-info "Concurrency conflict"
                                 {:aggregate-id     aggregate-id
                                  :expected-version expected-version
                                  :actual-version   current-version})))
               (let [numbered (map-indexed
                                (fn [i ev]
                                  (assoc ev
                                         :aggregate-id aggregate-id
                                         :sequence-number (+ seq i 1)))
                                new-events)]
                 (-> s
                     (update :events into numbered)
                     (assoc :seq (+ seq (count new-events))))))))
    nil)
  (-get-events [_ aggregate-id after-version]
    (->> (:events @state)
         (filter #(and (= aggregate-id (:aggregate-id %))
                       (> (:version %) after-version)))
         (sort-by :version)
         vec))
  (-get-all-events [_ after-sequence limit]
    (let [xs (->> (:events @state)
                  (filter #(> (:sequence-number %) after-sequence))
                  (sort-by :sequence-number))]
      (vec (if limit (take limit xs) xs))))
  (-save-snapshot! [_ snapshot]
    (swap! state assoc-in [:snapshots (:aggregate-id snapshot)] snapshot)
    nil)
  (-get-latest-snapshot [_ aggregate-id]
    (get-in @state [:snapshots aggregate-id])))

(defn create-in-memory-store
  "Creates a fast, disk-free atom-backed EventStore for tests and demos."
  []
  (->InMemoryEventStore (atom {:events [] :seq 0 :snapshots {}})))

;; ---------------------------------------------------------------------------
;; Public store operations (dispatch on EventStore vs raw datasource)
;; ---------------------------------------------------------------------------

(defn append-events!
  "Appends a batch of events with optimistic concurrency. Accepts an
   EventStore or a raw SQLite datasource."
  [store aggregate-id events expected-version]
  (if (event-store? store)
    (-append-events! store aggregate-id events expected-version)
    (sqlite-append-events! store aggregate-id events expected-version)))

(defn get-events
  "Retrieves events for an aggregate with version > after-version (default 0).
   Accepts an EventStore or a raw SQLite datasource."
  ([store aggregate-id] (get-events store aggregate-id 0))
  ([store aggregate-id after-version]
   (if (event-store? store)
     (-get-events store aggregate-id after-version)
     (sqlite-get-events store aggregate-id after-version))))

(defn get-all-events
  "Retrieves events across all aggregates with sequence > after-sequence,
   optionally bounded by limit. Accepts an EventStore or raw datasource."
  ([store] (get-all-events store 0 nil))
  ([store after-sequence] (get-all-events store after-sequence nil))
  ([store after-sequence limit]
   (if (event-store? store)
     (-get-all-events store after-sequence limit)
     (sqlite-get-all-events store after-sequence limit))))

(defn save-snapshot!
  "Upserts a snapshot. Accepts an EventStore or a raw SQLite datasource."
  [store snapshot]
  (if (event-store? store)
    (-save-snapshot! store snapshot)
    (sqlite-save-snapshot! store snapshot)))

(defn get-latest-snapshot
  "Returns the latest snapshot for an aggregate, or nil. Accepts an
   EventStore or a raw SQLite datasource."
  [store aggregate-id]
  (if (event-store? store)
    (-get-latest-snapshot store aggregate-id)
    (sqlite-get-latest-snapshot store aggregate-id)))

;; ---------------------------------------------------------------------------
;; Aggregate repository
;; ---------------------------------------------------------------------------

(defn load-aggregate
  "Loads an aggregate by replaying its event stream. If a snapshot exists,
   only events after the snapshot version are replayed.

   Parameters:
     ds        - datasource
     initial   - initial empty aggregate map (must implement AggregateRoot)
     agg-id    - aggregate identifier string"
  [ds initial agg-id]
  (let [snapshot (get-latest-snapshot ds agg-id)
        base (if snapshot
               (agg/restore-from-snapshot initial snapshot)
               (assoc initial :aggregate-id agg-id :version 0 :uncommitted []))
        after-version (:version base 0)
        events (get-events ds agg-id after-version)]
    (agg/replay-events base events)))

(defn save-aggregate!
  "Persists all uncommitted events from the aggregate to the event store.
   Creates a snapshot if the snapshot interval has been reached.
   Returns the aggregate with uncommitted events cleared.

   Parameters:
     ds                - datasource
     aggregate         - the aggregate map
     snapshot-interval - number of events between snapshots (0 to disable)"
  ([ds aggregate] (save-aggregate! ds aggregate 50))
  ([ds aggregate snapshot-interval]
   (let [uncommitted (agg/get-uncommitted-events aggregate)]
     (when (seq uncommitted)
       (let [agg-id (:aggregate-id aggregate)
             expected-version (- (:version aggregate) (count uncommitted))]
         (append-events! ds agg-id uncommitted expected-version)
         (when (and (pos? snapshot-interval)
                    (zero? (mod (:version aggregate) snapshot-interval)))
           (save-snapshot! ds (agg/create-snapshot aggregate)))))
     (agg/clear-uncommitted-events aggregate))))
