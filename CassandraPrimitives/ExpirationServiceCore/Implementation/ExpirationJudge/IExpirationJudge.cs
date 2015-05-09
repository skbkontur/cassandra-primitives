namespace SKBKontur.Catalogue.CassandraPrimitives.ExpirationServiceCore.Implementation.ExpirationJudge
{
    public interface IExpirationJudge
    {
        ExpirationJudgeVerdict Judge(ExpiringObjectState currentState, long? newTimestamp);
    }
}