namespace EventSourcing.Store;

/// <summary>
/// Internal DTO for rows in the Events table. Used by Dapper for mapping.
/// </summary>
internal sealed class EventStoreRecord
{
    public long SequenceNumber { get; set; }
    public string AggregateId { get; set; } = string.Empty;
    public long Version { get; set; }
    public string EventType { get; set; } = string.Empty;
    public string Payload { get; set; } = string.Empty;
    public string Timestamp { get; set; } = string.Empty;
    public string EventId { get; set; } = string.Empty;
}
