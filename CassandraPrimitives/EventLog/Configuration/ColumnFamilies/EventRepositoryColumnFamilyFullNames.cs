using SkbKontur.Cassandra.Primitives.Storages.Primitives;

namespace SkbKontur.Cassandra.Primitives.EventLog.Configuration.ColumnFamilies
{
    public class EventRepositoryColumnFamilyFullNames : IEventRepositoryColumnFamilyFullNames
    {
        public EventRepositoryColumnFamilyFullNames(
            ColumnFamilyFullName eventLog,
            ColumnFamilyFullName eventLogAdditionalInfo,
            ColumnFamilyFullName remoteLock)
        {
            EventLog = eventLog;
            EventLogAdditionalInfo = eventLogAdditionalInfo;
            RemoteLock = remoteLock;
        }

        public ColumnFamilyFullName EventLog { get; }
        public ColumnFamilyFullName EventLogAdditionalInfo { get; }
        public ColumnFamilyFullName RemoteLock { get; }
    }
}