using System.Collections.Concurrent;
using System.Reflection;

namespace EventFlux.Saga;

/// <summary>
/// Converts event types to and from the version-less names SagaFlux stores (<c>"Namespace.Type, Assembly"</c>),
/// honouring <see cref="SagaEventFormerNameAttribute"/> for renamed or moved types.
/// </summary>
public static class SagaEventTypeNames
{
    private static readonly ConcurrentDictionary<string, Type> FormerNames = new(StringComparer.Ordinal);
    private static readonly ConcurrentDictionary<Assembly, bool> ScannedAssemblies = new();

    /// <summary>
    /// Gets the stored name of <paramref name="type"/>.
    /// </summary>
    /// <param name="type">The event type.</param>
    /// <returns><c>"Namespace.Type, Assembly"</c>.</returns>
    public static string GetName(Type type)
    {
        ArgumentNullException.ThrowIfNull(type);
        return $"{type.FullName}, {type.Assembly.GetName().Name}";
    }

    /// <summary>
    /// Resolves a stored name to the current type: the type itself when it still exists under that name, otherwise
    /// a type of a loaded assembly that declares it with <see cref="SagaEventFormerNameAttribute"/>.
    /// </summary>
    /// <param name="name">The stored name.</param>
    /// <returns>The type, or <c>null</c> when none matches.</returns>
    public static Type? Resolve(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        if (Type.GetType(name, throwOnError: false) is { } current)
            return current;

        if (FormerNames.TryGetValue(name, out var renamed))
            return renamed;

        ScanLoadedAssemblies();
        return FormerNames.TryGetValue(name, out renamed) ? renamed : null;
    }

    private static void ScanLoadedAssemblies()
    {
        foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
        {
            if (assembly.IsDynamic || !ScannedAssemblies.TryAdd(assembly, true))
                continue;

            Type[] types;
            try { types = assembly.GetTypes(); }
            catch (ReflectionTypeLoadException ex) { types = ex.Types.OfType<Type>().ToArray(); }

            foreach (var type in types)
                foreach (var attribute in type.GetCustomAttributes<SagaEventFormerNameAttribute>(inherit: false))
                    FormerNames.TryAdd(attribute.Name, type);
        }
    }
}
