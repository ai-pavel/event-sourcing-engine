using System.Reflection;
using EventSourcing.Core;

namespace EventSourcing.Store;

/// <summary>
/// Registry that maps event type names to CLR types for deserialization.
/// Scans loaded assemblies for concrete <see cref="DomainEvent"/> subclasses.
/// </summary>
public sealed class EventTypeRegistry
{
    private readonly Dictionary<string, Type> _typeMap = new();

    /// <summary>
    /// Creates a registry by scanning the provided assemblies (or all loaded assemblies)
    /// for concrete subclasses of <see cref="DomainEvent"/>.
    /// </summary>
    public EventTypeRegistry(params Assembly[] assemblies)
    {
        var assembliesToScan = assemblies.Length > 0
            ? assemblies
            : AppDomain.CurrentDomain.GetAssemblies();

        foreach (var assembly in assembliesToScan)
        {
            try
            {
                foreach (var type in assembly.GetTypes())
                {
                    if (type is { IsAbstract: false, IsClass: true } && type.IsAssignableTo(typeof(DomainEvent)))
                    {
                        var fullName = type.FullName ?? type.Name;
                        _typeMap[fullName] = type;
                    }
                }
            }
            catch (ReflectionTypeLoadException)
            {
                // Skip assemblies that cannot be loaded
            }
        }
    }

    /// <summary>Registers a specific event type manually.</summary>
    public void Register<TEvent>() where TEvent : DomainEvent
    {
        var type = typeof(TEvent);
        _typeMap[type.FullName ?? type.Name] = type;
    }

    /// <summary>Resolves a CLR type from a stored event type name.</summary>
    public Type? Resolve(string eventTypeName)
    {
        return _typeMap.GetValueOrDefault(eventTypeName);
    }
}
