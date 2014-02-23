using SKBKontur.Catalogue.CassandraPrimitives.Storages.Primitives;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.Configuration.ColumnFamilies
{
    public class EventRepositoryColumnFamilyFullNames : IEventRepositoryColumnFamilyFullNames
    {
        public EventRepositoryColumnFamilyFullNames(
            ColumnFamilyFullName ticksHolder,
            ColumnFamilyFullName eventLog,
            ColumnFamilyFullName eventLogAdditionalInfo,
            ColumnFamilyFullName eventMeta,
            ColumnFamilyFullName remoteLock)
        {
            TicksHolder = ticksHolder;
            EventLog = eventLog;
            EventLogAdditionalInfo = eventLogAdditionalInfo;
            EventMeta = eventMeta;
            RemoteLock = remoteLock;
        }

        public ColumnFamilyFullName TicksHolder { get; private set; }
        public ColumnFamilyFullName EventLog { get; private set; }
        public ColumnFamilyFullName EventLogAdditionalInfo { get; private set; }
        public ColumnFamilyFullName EventMeta { get; private set; }
        public ColumnFamilyFullName RemoteLock { get; private set; }
    }
}