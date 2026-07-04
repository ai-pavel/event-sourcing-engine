using EventSourcing.Core;
using EventSourcing.Store;

namespace EventSourcing.Projections;

/// <summary>
/// Engine that subscribes to stored events and dispatches them to registered projections.
/// Supports both catch-up (replaying all past events) and live processing modes.
/// </summary>
public sealed class ProjectionEngine
{
    private readonly IEventStore _eventStore;
    private readonly List<IProjection> _projections = new();
    private long _lastProcessedSequence;

    public ProjectionEngine(IEventStore eventStore)
    {
        _eventStore = eventStore;
    }

    /// <summary>Registers a projection to receive events.</summary>
    public ProjectionEngine Register(IProjection projection)
    {
        _projections.Add(projection);
        return this;
    }

    /// <summary>
    /// Catches up by replaying all events that have not yet been processed,
    /// dispatching each to all registered projections.
    /// </summary>
    public async Task CatchUpAsync()
    {
        var events = await _eventStore.GetAllEventsAsync(_lastProcessedSequence);

        foreach (var @event in events)
        {
            foreach (var projection in _projections)
            {
                await projection.HandleAsync(@event);
            }

            _lastProcessedSequence = @event.Version;
        }
    }

    /// <summary>
    /// Runs a continuous polling loop that periodically catches up on new events.
    /// </summary>
    /// <param name="pollingInterval">Time between poll cycles.</param>
    /// <param name="cancellationToken">Token to stop the loop.</param>
    public async Task RunAsync(TimeSpan pollingInterval, CancellationToken cancellationToken = default)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            await CatchUpAsync();

            try
            {
                await Task.Delay(pollingInterval, cancellationToken);
            }
            catch (TaskCanceledException)
            {
                break;
            }
        }
    }
}
