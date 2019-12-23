using SkbKontur.Cassandra.Primitives.EventLog.Primitives;

namespace SkbKontur.Cassandra.Primitives.EventLog.Implementation
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