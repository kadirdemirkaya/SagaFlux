using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Text;

namespace EventFlux.Saga.Generators;

internal sealed class SagaStateSetModel : IEquatable<SagaStateSetModel>
{
    public SagaStateSetModel(string? ns, string enumName, string enumFullName, bool isPublic, bool hasAliases,
        bool isAccessible, bool hasGenericContainer, string[] members, decimal[] values,
        string filePath, TextSpan span, LinePositionSpan lineSpan)
    {
        Namespace = ns;
        EnumName = enumName;
        EnumFullName = enumFullName;
        IsPublic = isPublic;
        HasAliases = hasAliases;
        IsAccessible = isAccessible;
        HasGenericContainer = hasGenericContainer;
        Members = members;
        Values = values;
        FilePath = filePath;
        Span = span;
        LineSpan = lineSpan;
    }

    public string? Namespace { get; }

    public string EnumName { get; }

    public string EnumFullName { get; }

    public bool IsPublic { get; }

    public bool HasAliases { get; }

    public bool IsAccessible { get; }

    public bool HasGenericContainer { get; }

    public string[] Members { get; }

    public decimal[] Values { get; }

    public string FilePath { get; }

    public TextSpan Span { get; }

    public LinePositionSpan LineSpan { get; }

    public Location ToLocation() => Location.Create(FilePath, Span, LineSpan);

    public bool Equals(SagaStateSetModel? other)
        => other is not null
           && Namespace == other.Namespace
           && EnumFullName == other.EnumFullName
           && IsPublic == other.IsPublic
           && HasAliases == other.HasAliases
           && IsAccessible == other.IsAccessible
           && HasGenericContainer == other.HasGenericContainer
           && Members.SequenceEqual(other.Members)
           && Values.SequenceEqual(other.Values)
           && FilePath == other.FilePath
           && Span == other.Span;

    public override bool Equals(object? obj) => Equals(obj as SagaStateSetModel);

    public override int GetHashCode()
    {
        unchecked
        {
            var hash = EnumFullName.GetHashCode();
            foreach (var member in Members)
                hash = hash * 31 + member.GetHashCode();
            return hash;
        }
    }
}
