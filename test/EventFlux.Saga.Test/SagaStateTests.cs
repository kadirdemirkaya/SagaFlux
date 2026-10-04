namespace EventFlux.Saga.Test;

public class SagaStateTests
{
    [Fact]
    public void States_with_same_name_are_equal_regardless_of_ordinal()
    {
        Assert.Equal(new SagaState("Paid", 1), new SagaState("Paid", 7));
        Assert.NotEqual(new SagaState("Paid", 1), new SagaState("paid", 1));
    }

    [Fact]
    public void Define_assigns_increasing_ordinals()
    {
        var a = SagaStates.Define("A");
        var b = SagaStates.Define("B");

        Assert.True(b.Ordinal > a.Ordinal);
        Assert.Equal("A", (string)a);
    }

    [Fact]
    public void Define_returns_well_known_states_for_their_names()
    {
        Assert.Equal(0, SagaStates.Define("Initial").Ordinal);
        Assert.Equal(SagaStates.Final, SagaStates.Define("Final"));
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void Define_rejects_blank_names(string name)
    {
        Assert.Throws<ArgumentException>(() => SagaStates.Define(name));
    }
}
