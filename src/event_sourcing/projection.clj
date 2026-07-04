(ns event-sourcing.projection
  "Projection engine that subscribes to stored events and builds read models.

   Projections are functions that take (state, event) -> state, similar to
   reducers. Read models are maintained in atoms for thread-safe updates.")

(defprotocol Projection
  "Protocol for read-model projections that process domain events."
  (handle-event [projection event]
    "Processes a single domain event to update the projection's read model."))

(defn create-projection-engine
  "Creates a projection engine that tracks which events have been processed.
   Returns a map with:
     :projections         - atom holding registered projection instances
     :last-processed-seq  - atom tracking the last processed sequence number"
  []
  {:projections        (atom [])
   :last-processed-seq (atom 0)})

(defn register!
  "Registers a projection with the engine. Returns the engine for chaining."
  [engine projection]
  (swap! (:projections engine) conj projection)
  engine)

(defn catch-up!
  "Catches up by replaying all events that have not yet been processed,
   dispatching each to all registered projections.

   Parameters:
     engine     - the projection engine
     get-events - fn that takes a sequence number and returns events after it"
  [engine get-events-fn]
  (let [events (get-events-fn @(:last-processed-seq engine))
        projections @(:projections engine)]
    (doseq [event events]
      (doseq [proj projections]
        (handle-event proj event))
      (reset! (:last-processed-seq engine)
              (or (:sequence-number event) (:version event))))))
