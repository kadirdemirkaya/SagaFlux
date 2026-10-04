using System.Text.Json;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace EventFlux.Saga.EfCore;

internal sealed class JoinedEventsConverter : ValueConverter<List<string>, string>
{
    public static readonly JoinedEventsConverter Instance = new();

    public static readonly ValueComparer<List<string>> Comparer = new(
        (a, b) => (a == null && b == null) || (a != null && b != null && a.SequenceEqual(b)),
        list => list.Aggregate(0, (hash, entry) => HashCode.Combine(hash, entry.GetHashCode())),
        list => list.ToList());

    private JoinedEventsConverter()
        : base(
            list => JsonSerializer.Serialize(list, (JsonSerializerOptions?)null),
            json => JsonSerializer.Deserialize<List<string>>(json, (JsonSerializerOptions?)null) ?? new List<string>())
    {
    }
}
