using EfCore.Repository.Concretes;

namespace EventFlux.Saga.EfCore.Test;

public sealed class CompensationPersistenceTests : IDisposable
{
    private readonly SqliteDatabase _db = new();

    private static EfCoreSagaRepository<TripState> Repository(SagaDbContext context)
        => new(new ReadRepository<TripState>(context, null!), new WriteRepository<TripState>(context, null!));

    [Fact]
    public async Task Compensation_stack_round_trips_and_marks_are_persisted()
    {
        var id = Guid.NewGuid();
        var entered = DateTimeOffset.UtcNow;
        await using (var context = _db.CreateContext())
        {
            var state = new TripState
            {
                CorrelationId = id,
                CurrentState = "AwaitingHotel",
                Compensations = { new CompensationEntry("AwaitingHotel", "CancelFlight", entered) }
            };
            await Repository(context).SaveAsync(state, default);
        }

        await using (var context = _db.CreateContext())
        {
            var repository = Repository(context);
            var loaded = (await repository.FindAsync(id, default))!;
            var entry = Assert.Single(loaded.Compensations);
            Assert.Equal("CancelFlight", entry.CompensationActionKey);
            Assert.Equal(entered, entry.EnteredAt);

            loaded.Compensations[0] = entry with { CompensatedAt = DateTimeOffset.UtcNow };
            loaded.Compensations.Add(new CompensationEntry("AwaitingCar", "CancelHotel", DateTimeOffset.UtcNow));
            await repository.SaveAsync(loaded, default);
        }

        await using (var context = _db.CreateContext())
        {
            var loaded = (await Repository(context).FindAsync(id, default))!;
            Assert.Equal(2, loaded.Compensations.Count);
            Assert.True(loaded.Compensations[0].IsCompensated);
            Assert.False(loaded.Compensations[1].IsCompensated);
            Assert.Equal(2, loaded.Version);
        }
    }

    public void Dispose() => _db.Dispose();
}
