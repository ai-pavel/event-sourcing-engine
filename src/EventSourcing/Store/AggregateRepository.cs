using EventSourcing.Core;

namespace EventSourcing.Store;

/// <summary>
/// Repository that loads and saves event-sourced aggregates.
/// Handles snapshot-based optimization and periodic snapshot creation.
/// </summary>
/// <typeparam name="TAggregate">The aggregate root type.</typeparam>
/// <typeparam name="TId">The aggregate identifier type.</typeparam>
public sealed class AggregateRepository<TAggregate, TId>
    where TAggregate : AggregateRoot<TId>, new()
    where TId : notnull
{
    private readonly IEventStore _eventStore;
    private readonly int _snapshotInterval;

    /// <summary>
    /// Creates a new repository instance.
    /// </summary>
    /// <param name="eventStore">The underlying event store.</param>
    /// <param name="snapshotInterval">
    /// Number of events between automatic snapshots. Set to 0 to disable auto-snapshotting.
    /// </param>
    public AggregateRepository(IEventStore eventStore, int snapshotInterval = 50)
    {
        _eventStore = eventStore;
        _snapshotInterval = snapshotInterval;
    }

    /// <summary>
    /// Loads an aggregate by replaying its event stream. If a snapshot exists,
    /// only events after the snapshot version are replayed.
    /// </summary>
    public async Task<TAggregate> LoadAsync(string aggregateId)
    {
        var aggregate = new TAggregate();

        // Try to load from snapshot first
        var snapshot = await _eventStore.GetLatestSnapshotAsync(aggregateId);
        long afterVersion = 0;

        if (snapshot is not null)
        {
            aggregate.RestoreFromSnapshot(snapshot);
            afterVersion = snapshot.Version;
        }

        // Replay events that occurred after the snapshot
        var events = await _eventStore.GetEventsAsync(aggregateId, afterVersion);

        foreach (var @event in events)
        {
            aggregate.ReplayEvent(@event);
        }

        return aggregate;
    }

    /// <summary>
    /// Persists all uncommitted events from the aggregate to the event store.
    /// Creates a snapshot if the snapshot interval has been reached.
    /// </summary>
    public async Task SaveAsync(TAggregate aggregate)
    {
        var uncommittedEvents = aggregate.GetUncommittedEvents();
        if (uncommittedEvents.Count == 0)
        {
            return;
        }

        var aggregateId = aggregate.Id?.ToString() ?? string.Empty;
        var expectedVersion = aggregate.Version - uncommittedEvents.Count;

        await _eventStore.AppendEventsAsync(aggregateId, uncommittedEvents, expectedVersion);

        // Create snapshot if interval is reached
        if (_snapshotInterval > 0 && aggregate.Version % _snapshotInterval == 0)
        {
            var snapshot = aggregate.CreateSnapshot();
            await _eventStore.SaveSnapshotAsync(snapshot);
        }

        aggregate.ClearUncommittedEvents();
    }
}
