using SKBKontur.Catalogue.CassandraPrimitives.Storages.Primitives;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.External
{
    public interface IEventRepositoryColumnFamilyFullNames
    {
        ColumnFamilyFullName TicksHolder { get; }
        ColumnFamilyFullName EventLog { get; }
        ColumnFamilyFullName EventLogAdditionalInfo { get; }
        ColumnFamilyFullName EventMeta { get; }
        ColumnFamilyFullName RemoteLock { get; }
    }
}