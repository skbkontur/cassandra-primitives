using SKBKontur.Catalogue.CassandraPrimitives.Storages.Primitives;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.Configuration.ColumnFamilies
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