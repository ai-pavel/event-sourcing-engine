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

(def ^:const default-page-size
  "Default number of events fetched per page during catch-up."
  500)

(defn- fetch-page
  "Calls get-events-fn for one page. Supports both a paging fn of arity
   [after-seq limit] and a legacy fn of arity [after-seq] (which returns the
   whole tail; treated as a single final page)."
  [get-events-fn after-seq page-size]
  (try
    (get-events-fn after-seq page-size)
    (catch clojure.lang.ArityException _
      (get-events-fn after-seq))))

(defn catch-up!
  "Catches up by replaying events that have not yet been processed,
   dispatching each to all registered projections.

   Events are fetched in fixed-size pages so an arbitrarily long log is never
   loaded into memory all at once. Pages are fetched until one returns fewer
   rows than the page size. :last-processed-seq advances after each event.

   Parameters:
     engine        - the projection engine
     get-events-fn - fn of [after-seq limit] returning up to `limit` events
                     after `after-seq` (a legacy [after-seq] fn is also
                     accepted and treated as a single page)
     opts          - optional map; :page-size overrides the default page size"
  ([engine get-events-fn] (catch-up! engine get-events-fn {}))
  ([engine get-events-fn {:keys [page-size] :or {page-size default-page-size}}]
   (let [projections @(:projections engine)]
     (loop []
       (let [after-seq @(:last-processed-seq engine)
             events (fetch-page get-events-fn after-seq page-size)]
         (doseq [event events]
           (doseq [proj projections]
             (handle-event proj event))
           (reset! (:last-processed-seq engine)
                   (or (:sequence-number event) (:version event))))
         ;; A short (or empty) page means we have reached the end of the log.
         (when (= (count events) page-size)
           (recur)))))))
