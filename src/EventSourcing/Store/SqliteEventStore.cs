using System.Text.Json;
using Dapper;
using EventSourcing.Core;
using Microsoft.Data.Sqlite;

namespace EventSourcing.Store;

/// <summary>
/// SQLite-backed event store implementation using Dapper for data access.
/// Supports appending events with optimistic concurrency, loading event streams,
/// and managing snapshots.
/// </summary>
public sealed class SqliteEventStore : IEventStore, IDisposable
{
    private readonly string _connectionString;
    private readonly EventTypeRegistry _typeRegistry;

    public SqliteEventStore(string connectionString, EventTypeRegistry typeRegistry)
    {
        _connectionString = connectionString;
        _typeRegistry = typeRegistry;
    }

    /// <summary>
    /// Creates the required database tables if they do not already exist.
    /// Call this once during application startup.
    /// </summary>
    public async Task InitializeAsync()
    {
        using var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync();

        await connection.ExecuteAsync("""
            CREATE TABLE IF NOT EXISTS Events (
                SequenceNumber INTEGER PRIMARY KEY AUTOINCREMENT,
                AggregateId    TEXT    NOT NULL,
                Version        INTEGER NOT NULL,
                EventType      TEXT    NOT NULL,
                Payload        TEXT    NOT NULL,
                Timestamp      TEXT    NOT NULL,
                EventId        TEXT    NOT NULL,
                UNIQUE(AggregateId, Version)
            );

            CREATE INDEX IF NOT EXISTS IX_Events_AggregateId ON Events(AggregateId);

            CREATE TABLE IF NOT EXISTS Snapshots (
                AggregateId   TEXT    NOT NULL,
                Version       INTEGER NOT NULL,
                AggregateType TEXT    NOT NULL,
                State         TEXT    NOT NULL,
                Timestamp     TEXT    NOT NULL,
                PRIMARY KEY (AggregateId)
            );
            """);
    }

    public async Task AppendEventsAsync(string aggregateId, IEnumerable<DomainEvent> events, long expectedVersion)
    {
        using var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync();
        using var transaction = await connection.BeginTransactionAsync();

        try
        {
            // Optimistic concurrency check
            var currentVersion = await connection.ExecuteScalarAsync<long?>(
                "SELECT MAX(Version) FROM Events WHERE AggregateId = @AggregateId",
                new { AggregateId = aggregateId },
                transaction);

            var actualVersion = currentVersion ?? 0;
            if (actualVersion != expectedVersion)
            {
                throw new ConcurrencyException(aggregateId, expectedVersion, actualVersion);
            }

            foreach (var @event in events)
            {
                var payload = JsonSerializer.Serialize(@event, @event.GetType());

                await connection.ExecuteAsync(
                    """
                    INSERT INTO Events (AggregateId, Version, EventType, Payload, Timestamp, EventId)
                    VALUES (@AggregateId, @Version, @EventType, @Payload, @Timestamp, @EventId)
                    """,
                    new
                    {
                        AggregateId = aggregateId,
                        @event.Version,
                        EventType = @event.EventType,
                        Payload = payload,
                        Timestamp = @event.Timestamp.ToString("O"),
                        EventId = @event.EventId.ToString()
                    },
                    transaction);
            }

            transaction.Commit();
        }
        catch
        {
            transaction.Rollback();
            throw;
        }
    }

    public async Task<IReadOnlyList<DomainEvent>> GetEventsAsync(string aggregateId, long afterVersion = 0)
    {
        using var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync();

        var records = await connection.QueryAsync<EventStoreRecord>(
            """
            SELECT SequenceNumber, AggregateId, Version, EventType, Payload, Timestamp, EventId
            FROM Events
            WHERE AggregateId = @AggregateId AND Version > @AfterVersion
            ORDER BY Version
            """,
            new { AggregateId = aggregateId, AfterVersion = afterVersion });

        return DeserializeRecords(records).ToList().AsReadOnly();
    }

    public async Task<IReadOnlyList<DomainEvent>> GetAllEventsAsync(long afterSequenceNumber = 0)
    {
        using var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync();

        var records = await connection.QueryAsync<EventStoreRecord>(
            """
            SELECT SequenceNumber, AggregateId, Version, EventType, Payload, Timestamp, EventId
            FROM Events
            WHERE SequenceNumber > @AfterSequence
            ORDER BY SequenceNumber
            """,
            new { AfterSequence = afterSequenceNumber });

        return DeserializeRecords(records).ToList().AsReadOnly();
    }

    public async Task SaveSnapshotAsync(Snapshot snapshot)
    {
        using var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync();

        await connection.ExecuteAsync(
            """
            INSERT INTO Snapshots (AggregateId, Version, AggregateType, State, Timestamp)
            VALUES (@AggregateId, @Version, @AggregateType, @State, @Timestamp)
            ON CONFLICT(AggregateId) DO UPDATE SET
                Version = @Version,
                AggregateType = @AggregateType,
                State = @State,
                Timestamp = @Timestamp
            """,
            new
            {
                snapshot.AggregateId,
                snapshot.Version,
                snapshot.AggregateType,
                snapshot.State,
                Timestamp = snapshot.Timestamp.ToString("O")
            });
    }

    public async Task<Snapshot?> GetLatestSnapshotAsync(string aggregateId)
    {
        using var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync();

        return await connection.QueryFirstOrDefaultAsync<Snapshot>(
            """
            SELECT AggregateId, Version, AggregateType, State, Timestamp
            FROM Snapshots
            WHERE AggregateId = @AggregateId
            """,
            new { AggregateId = aggregateId });
    }

    private IEnumerable<DomainEvent> DeserializeRecords(IEnumerable<EventStoreRecord> records)
    {
        foreach (var record in records)
        {
            var eventType = _typeRegistry.Resolve(record.EventType);
            if (eventType is null)
            {
                throw new InvalidOperationException(
                    $"Unknown event type '{record.EventType}'. Register it with EventTypeRegistry.");
            }

            var @event = JsonSerializer.Deserialize(record.Payload, eventType) as DomainEvent;
            if (@event is null)
            {
                throw new InvalidOperationException(
                    $"Failed to deserialize event '{record.EventId}' of type '{record.EventType}'.");
            }

            @event.Version = record.Version;
            yield return @event;
        }
    }

    public void Dispose()
    {
        // SqliteConnection instances are created per-call, nothing to dispose at store level.
    }
}
