using SkbKontur.Cassandra.Primitives.Storages.Primitives;

namespace SkbKontur.Cassandra.Primitives.EventLog.Configuration.ColumnFamilies
{
    public interface IEventRepositoryColumnFamilyFullNames
    {
        ColumnFamilyFullName EventLog { get; }
        ColumnFamilyFullName EventLogAdditionalInfo { get; }
        ColumnFamilyFullName RemoteLock { get; }
    }
}