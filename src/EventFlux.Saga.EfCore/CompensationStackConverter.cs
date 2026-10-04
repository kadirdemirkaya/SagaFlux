using System.Text.Json;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace EventFlux.Saga.EfCore;

internal sealed class CompensationStackConverter : ValueConverter<List<CompensationEntry>, string>
{
    public static readonly CompensationStackConverter Instance = new();

    public static readonly ValueComparer<List<CompensationEntry>> Comparer = new(
        (a, b) => (a == null && b == null) || (a != null && b != null && a.SequenceEqual(b)),
        list => list.Aggregate(0, (hash, entry) => HashCode.Combine(hash, entry.GetHashCode())),
        list => list.ToList());

    private CompensationStackConverter()
        : base(
            list => JsonSerializer.Serialize(list, (JsonSerializerOptions?)null),
            json => JsonSerializer.Deserialize<List<CompensationEntry>>(json, (JsonSerializerOptions?)null) ?? new List<CompensationEntry>())
    {
    }
}
