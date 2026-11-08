using System.ComponentModel;
using System.Reflection;
using EventFlux.Saga.Test.Fixtures;

namespace EventFlux.Saga.Test;

public class EngineSurfaceVisibilityTests
{
    private static readonly string[] HiddenSagaMembers =
    [
        nameof(Saga<OrderState>.EventTypes),
        nameof(Saga<OrderState>.RemovesWhenFinal),
        nameof(Saga<OrderState>.TimeoutTypes),
        nameof(Saga<OrderState>.CompensationKeys),
        nameof(Saga<OrderState>.Validate),
        nameof(Saga<OrderState>.Handles),
        nameof(Saga<OrderState>.GetCorrelationId),
    ];

    private static readonly string[] HiddenPublishedEventMembers =
    [
        nameof(SagaPublishedEvent.FromEvent),
        nameof(SagaPublishedEvent.SendAsync),
    ];

    [Fact]
    public void Saga_engine_members_are_public_and_hidden_from_intellisense()
        => AssertHiddenExactly(typeof(Saga<>), HiddenSagaMembers);

    [Fact]
    public void Published_event_engine_members_are_public_and_hidden_from_intellisense()
        => AssertHiddenExactly(typeof(SagaPublishedEvent), HiddenPublishedEventMembers);

    [Fact]
    public void ExecuteAsync_stays_visible()
    {
        var overloads = typeof(Saga<>).GetMethods(BindingFlags.Public | BindingFlags.Instance)
            .Where(m => m.Name == nameof(Saga<OrderState>.ExecuteAsync))
            .ToList();

        Assert.Equal(2, overloads.Count);
        Assert.All(overloads, m => Assert.Null(m.GetCustomAttribute<EditorBrowsableAttribute>()));
    }

    private static void AssertHiddenExactly(Type type, string[] expected)
    {
        const BindingFlags flags = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance
            | BindingFlags.Static | BindingFlags.DeclaredOnly;
        var members = type.GetMembers(flags);

        var hidden = members
            .Where(m => m.GetCustomAttribute<EditorBrowsableAttribute>()?.State == EditorBrowsableState.Never)
            .ToList();

        Assert.Equal(expected.OrderBy(n => n, StringComparer.Ordinal), hidden.Select(m => m.Name).OrderBy(n => n, StringComparer.Ordinal));
        Assert.All(hidden, m => Assert.True(IsPublic(m), $"{m.Name} must stay public."));
    }

    private static bool IsPublic(MemberInfo member) => member switch
    {
        MethodInfo method => method.IsPublic,
        PropertyInfo property => property.GetMethod is { IsPublic: true },
        _ => false,
    };
}
