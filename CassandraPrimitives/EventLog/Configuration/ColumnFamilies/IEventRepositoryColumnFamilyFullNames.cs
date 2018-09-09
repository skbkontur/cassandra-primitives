using SKBKontur.Catalogue.CassandraPrimitives.Storages.Primitives;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.Configuration.ColumnFamilies
{
    public interface IEventRepositoryColumnFamilyFullNames
    {
        ColumnFamilyFullName TicksHolder { get; }
        ColumnFamilyFullName EventLog { get; }
        ColumnFamilyFullName EventLogAdditionalInfo { get; }
        ColumnFamilyFullName RemoteLock { get; }
    }
}