using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Primitives;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.Implementation
{
    internal class ProcessResult
    {
        public ProcessResult(EventInfo[] successInfos, EventId[] failureIds)
        {
            this.successInfos = successInfos;
            this.failureIds = failureIds;
        }

        public readonly EventInfo[] successInfos;
        public readonly EventId[] failureIds;
    }
}