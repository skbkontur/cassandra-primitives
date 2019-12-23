using CassandraPrimitives.Tests.SchemeActualizer;

using SkbKontur.Cassandra.Primitives.Storages.Primitives;

namespace CassandraPrimitives.Tests.FunctionalTests.Settings
{
    public class CassandraMetaProvider : ICassandraMetadataProvider
    {
        public ColumnFamilyFullName[] GetColumnFamilies()
        {
            return new[] {ColumnFamilies.eventLog, ColumnFamilies.eventLogAdditionalInfo, ColumnFamilies.remoteLock};
        }
    }
}