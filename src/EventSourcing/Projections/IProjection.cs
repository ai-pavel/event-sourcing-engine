using EventSourcing.Core;

namespace EventSourcing.Projections;

/// <summary>
/// Defines a read-model projection that processes domain events to build
/// query-optimized views.
/// </summary>
public interface IProjection
{
    /// <summary>
    /// Processes a single domain event to update the projection's read model.
    /// </summary>
    Task HandleAsync(DomainEvent @event);
}
