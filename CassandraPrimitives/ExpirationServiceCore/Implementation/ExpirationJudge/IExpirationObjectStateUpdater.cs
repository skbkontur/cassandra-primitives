namespace SKBKontur.Catalogue.CassandraPrimitives.ExpirationServiceCore.Implementation.ExpirationJudge
{
    public interface IExpirationObjectStateUpdater
    {
        ExpiringObjectState Update(ExpiringObjectState currentState, long? newTimestamp);
    }
}