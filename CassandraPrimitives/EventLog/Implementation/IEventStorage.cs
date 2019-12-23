using SkbKontur.Cassandra.Primitives.EventLog.Primitives;

namespace SkbKontur.Cassandra.Primitives.EventLog.Implementation
{
    internal interface IEventStorage
    {
        void Write(EventLogRecord[] events, long timestamp, int? ttl = null);
        void Delete(EventInfo[] eventInfos, long timestamp);
    }
}