using System;

namespace SKBKontur.Catalogue.CassandraPrimitives.ExpirationServiceCore.Implementation.ExpirationJudge
{
    public class ExpirationObjectStateUpdater : IExpirationObjectStateUpdater
    {
        public ExpirationObjectStateUpdater(IExpirationJudge expirationJudge)
        {
            this.expirationJudge = expirationJudge;
        }

        public ExpiringObjectState Update(ExpiringObjectState currentState, long? newTimestamp)
        {
            var verdict = expirationJudge.Judge(currentState, newTimestamp);
            switch(verdict)
            {
            case ExpirationJudgeVerdict.Deleted:
                return null;
            case ExpirationJudgeVerdict.Expired:
                currentState.IsExpired = true;
                return currentState;
            case ExpirationJudgeVerdict.Ok:
                currentState = currentState ?? new ExpiringObjectState();
                currentState.Lifetime++;
                currentState.LastTimestamp = newTimestamp;
                return currentState;
            default:
                throw new Exception(string.Format("Unexpected verdict from judge {0}", verdict));
            }
        }

        private readonly IExpirationJudge expirationJudge;
    }
}