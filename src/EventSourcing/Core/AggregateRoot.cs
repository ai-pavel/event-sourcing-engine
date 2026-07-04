using System.Text.Json;

namespace EventSourcing.Core;

/// <summary>
/// Base class for event-sourced aggregates. Subclasses define their state transitions
/// by implementing Apply methods for each event type they produce.
/// </summary>
/// <typeparam name="TId">The type of the aggregate identifier.</typeparam>
public abstract class AggregateRoot<TId> where TId : notnull
{
    private readonly List<DomainEvent> _uncommittedEvents = new();

    /// <summary>The unique identifier for this aggregate instance.</summary>
    public TId Id { get; protected set; } = default!;

    /// <summary>
    /// The current version of the aggregate, incremented with each applied event.
    /// A newly created aggregate starts at version 0.
    /// </summary>
    public long Version { get; private set; }

    /// <summary>Returns the list of events that have not yet been persisted.</summary>
    public IReadOnlyList<DomainEvent> GetUncommittedEvents() => _uncommittedEvents.AsReadOnly();

    /// <summary>Clears the uncommitted events list after they have been persisted.</summary>
    public void ClearUncommittedEvents() => _uncommittedEvents.Clear();

    /// <summary>
    /// Raises a new domain event: assigns its version, applies it to mutate state,
    /// and adds it to the uncommitted events list.
    /// </summary>
    protected void RaiseEvent(DomainEvent @event)
    {
        ArgumentNullException.ThrowIfNull(@event);

        Version++;
        @event.Version = Version;

        ApplyEvent(@event);
        _uncommittedEvents.Add(@event);
    }

    /// <summary>
    /// Replays a previously persisted event to rebuild aggregate state.
    /// Updates the version but does not add to uncommitted events.
    /// </summary>
    internal void ReplayEvent(DomainEvent @event)
    {
        Version = @event.Version;
        ApplyEvent(@event);
    }

    /// <summary>
    /// Restores the aggregate from a snapshot, setting the version and
    /// deserializing state from the snapshot's JSON payload.
    /// </summary>
    internal void RestoreFromSnapshot(Snapshot snapshot)
    {
        Version = snapshot.Version;
        DeserializeState(snapshot.State);
    }

    /// <summary>
    /// Creates a snapshot of the aggregate's current state.
    /// </summary>
    public Snapshot CreateSnapshot()
    {
        return new Snapshot
        {
            AggregateId = Id?.ToString() ?? string.Empty,
            Version = Version,
            AggregateType = GetType().FullName ?? GetType().Name,
            State = SerializeState(),
            Timestamp = DateTimeOffset.UtcNow
        };
    }

    /// <summary>
    /// Applies a domain event to mutate the aggregate's internal state.
    /// Subclasses must implement this using pattern matching or dispatch.
    /// </summary>
    protected abstract void ApplyEvent(DomainEvent @event);

    /// <summary>
    /// Serializes the aggregate state to JSON for snapshot storage.
    /// Override in subclasses to customize serialization.
    /// </summary>
    protected virtual string SerializeState()
    {
        return JsonSerializer.Serialize(this, GetType(), JsonSerializerOptions.Default);
    }

    /// <summary>
    /// Deserializes aggregate state from a JSON snapshot payload.
    /// Subclasses must implement this to restore their specific state.
    /// </summary>
    protected abstract void DeserializeState(string json);
}
