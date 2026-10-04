using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Text;

namespace EventFlux.Saga.Generators;

internal sealed class SagaStateSetModel : IEquatable<SagaStateSetModel>
{
    public SagaStateSetModel(string? ns, string enumName, string enumFullName, bool isPublic, string[] members, long[] values,
        string filePath, TextSpan span, LinePositionSpan lineSpan)
    {
        Namespace = ns;
        EnumName = enumName;
        EnumFullName = enumFullName;
        IsPublic = isPublic;
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

    public string[] Members { get; }

    public long[] Values { get; }

    public string FilePath { get; }

    public TextSpan Span { get; }

    public LinePositionSpan LineSpan { get; }

    public Location ToLocation() => Location.Create(FilePath, Span, LineSpan);

    public bool Equals(SagaStateSetModel? other)
        => other is not null
           && Namespace == other.Namespace
           && EnumFullName == other.EnumFullName
           && IsPublic == other.IsPublic
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
