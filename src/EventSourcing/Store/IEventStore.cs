using EventSourcing.Core;

namespace EventSourcing.Store;

/// <summary>
/// Defines the contract for persisting and retrieving domain events and snapshots.
/// </summary>
public interface IEventStore
{
    /// <summary>Appends a batch of events for a given aggregate.</summary>
    Task AppendEventsAsync(string aggregateId, IEnumerable<DomainEvent> events, long expectedVersion);

    /// <summary>Retrieves all events for an aggregate, optionally starting after a given version.</summary>
    Task<IReadOnlyList<DomainEvent>> GetEventsAsync(string aggregateId, long afterVersion = 0);

    /// <summary>Retrieves all events across all aggregates, optionally starting after a global sequence number.</summary>
    Task<IReadOnlyList<DomainEvent>> GetAllEventsAsync(long afterSequenceNumber = 0);

    /// <summary>Saves a snapshot of an aggregate's state.</summary>
    Task SaveSnapshotAsync(Snapshot snapshot);

    /// <summary>Loads the most recent snapshot for an aggregate, if one exists.</summary>
    Task<Snapshot?> GetLatestSnapshotAsync(string aggregateId);
}
