namespace EventSourcing.Core;

/// <summary>
/// Represents a point-in-time snapshot of an aggregate's state.
/// Snapshots are used to avoid replaying the full event history when loading an aggregate.
/// </summary>
public class Snapshot
{
    /// <summary>The identifier of the aggregate this snapshot belongs to.</summary>
    public string AggregateId { get; init; } = string.Empty;

    /// <summary>The aggregate version at the time the snapshot was taken.</summary>
    public long Version { get; init; }

    /// <summary>The CLR type name of the aggregate, used for deserialization.</summary>
    public string AggregateType { get; init; } = string.Empty;

    /// <summary>Serialized aggregate state as JSON.</summary>
    public string State { get; init; } = string.Empty;

    /// <summary>UTC timestamp of when the snapshot was created.</summary>
    public DateTimeOffset Timestamp { get; init; } = DateTimeOffset.UtcNow;
}
