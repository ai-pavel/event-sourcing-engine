namespace EventSourcing.Store;

/// <summary>
/// Thrown when an optimistic concurrency conflict is detected while appending events.
/// This happens when the expected aggregate version does not match the actual stored version.
/// </summary>
public sealed class ConcurrencyException : Exception
{
    public string AggregateId { get; }
    public long ExpectedVersion { get; }
    public long ActualVersion { get; }

    public ConcurrencyException(string aggregateId, long expectedVersion, long actualVersion)
        : base($"Concurrency conflict for aggregate '{aggregateId}': expected version {expectedVersion}, but actual version is {actualVersion}.")
    {
        AggregateId = aggregateId;
        ExpectedVersion = expectedVersion;
        ActualVersion = actualVersion;
    }
}
