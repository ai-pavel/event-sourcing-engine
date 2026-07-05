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

(defn reset-engine!
  "Resets the engine's processed-sequence cursor back to zero so the next
   catch-up! rebuilds from the beginning of the log. Callers are responsible
   for reinitializing any stateful projection read models."
  [engine]
  (reset! (:last-processed-seq engine) 0)
  engine)

(defn catch-up!
  "Catches up by replaying all events that have not yet been processed,
   dispatching each to all registered projections.

   Each event is fully applied to every registered projection before the
   engine's :last-processed-seq advances, so a failure never leaves the
   cursor ahead of a partially-applied event. If a projection's handle-event
   throws, catch-up! rethrows a clear error identifying the failing event and
   projection index and leaves :last-processed-seq at the last event that was
   fully applied to all projections.

   Events must carry a global :sequence-number and be delivered in strictly
   increasing sequence order; catch-up! validates monotonicity.

   Parameters:
     engine     - the projection engine
     get-events - fn that takes a sequence number and returns events after it"
  [engine get-events-fn]
  (let [start-seq @(:last-processed-seq engine)
        events (get-events-fn start-seq)
        projections @(:projections engine)]
    (loop [remaining events
           last-seq start-seq]
      (if-let [event (first remaining)]
        (let [seq-num (:sequence-number event)]
          (when (nil? seq-num)
            (throw (ex-info "Event is missing a global :sequence-number"
                            {:event event})))
          (when (<= seq-num last-seq)
            (throw (ex-info "Events out of order in catch-up!"
                            {:last-processed-seq last-seq
                             :event-sequence     seq-num})))
          ;; Apply the event to every projection before advancing the cursor.
          (doseq [[idx proj] (map-indexed vector projections)]
            (try
              (handle-event proj event)
              (catch Throwable t
                (throw (ex-info "Projection failed while handling event"
                                {:projection-index idx
                                 :event-sequence   seq-num
                                 :event            event}
                                t)))))
          (reset! (:last-processed-seq engine) seq-num)
          (recur (rest remaining) seq-num))
        engine))))
