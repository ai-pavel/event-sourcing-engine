namespace EventSourcing.Core;

/// <summary>
/// Base class for all domain events. Each event captures a single state change
/// that occurred within an aggregate.
/// </summary>
public abstract class DomainEvent
{
    /// <summary>Unique identifier for this event instance.</summary>
    public Guid EventId { get; init; } = Guid.NewGuid();

    /// <summary>The identifier of the aggregate that produced this event.</summary>
    public string AggregateId { get; init; } = string.Empty;

    /// <summary>The sequential version number within the aggregate's event stream.</summary>
    public long Version { get; internal set; }

    /// <summary>UTC timestamp of when the event was created.</summary>
    public DateTimeOffset Timestamp { get; init; } = DateTimeOffset.UtcNow;

    /// <summary>
    /// The CLR type name of the event, used for deserialization.
    /// Defaults to the concrete class name.
    /// </summary>
    public string EventType => GetType().FullName ?? GetType().Name;
}
