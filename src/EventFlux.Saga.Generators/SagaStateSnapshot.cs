namespace EventFlux.Saga.Generators;

internal sealed class SagaStateSnapshot : IEquatable<SagaStateSnapshot>
{
    public const string Extension = ".sagastates";

    public SagaStateSnapshot(string enumName, string[] states)
    {
        EnumName = enumName;
        States = states;
    }

    public string EnumName { get; }

    public string[] States { get; }

    public static string? EnumNameOf(string path)
    {
        if (!path.EndsWith(Extension, StringComparison.OrdinalIgnoreCase))
            return null;
        var start = Math.Max(path.LastIndexOf('/'), path.LastIndexOf('\\')) + 1;
        return path.Substring(start, path.Length - start - Extension.Length);
    }

    public static string[] Parse(string text)
        => text.Split('\n')
            .Select(line => line.Trim())
            .Where(line => line.Length > 0 && !line.StartsWith("#", StringComparison.Ordinal))
            .Distinct(StringComparer.Ordinal)
            .ToArray();

    public bool Equals(SagaStateSnapshot? other)
        => other is not null && EnumName == other.EnumName && States.SequenceEqual(other.States);

    public override bool Equals(object? obj) => Equals(obj as SagaStateSnapshot);

    public override int GetHashCode() => EnumName.GetHashCode();
}
