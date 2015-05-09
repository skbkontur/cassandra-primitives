namespace SKBKontur.Catalogue.CassandraPrimitives.ExpirationServiceCore.Implementation.ExpirationJudge
{
    public class ExpirationJudge : IExpirationJudge
    {
        public ExpirationJudgeVerdict Judge(ExpiringObjectState currentState, long? newTimestamp)
        {
            if(currentState == null)
                return ExpirationJudgeVerdict.Ok;
            if(newTimestamp == null)
            {
                if(currentState.LastTimestamp != null)
                    return ExpirationJudgeVerdict.Deleted;
                if(currentState.Lifetime > 3)
                    return ExpirationJudgeVerdict.Deleted;
                return ExpirationJudgeVerdict.Ok;
            }
            if(currentState.LastTimestamp == newTimestamp)
                return ExpirationJudgeVerdict.Expired;
            return ExpirationJudgeVerdict.Ok;
        }
    }
}