using SKBKontur.Catalogue.CassandraPrimitives.Storages.Primitives;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.SchemeActualizer;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.EventLoggerBenchmark.Settings
{
    public class CassandraMetaProvider : ICassandraMetadataProvider
    {
        public ColumnFamilyFullName[] GetColumnFamilies()
        {
            return new[] {ColumnFamilies.eventLog, ColumnFamilies.ticksHolder, ColumnFamilies.eventLogAdditionalInfo, ColumnFamilies.remoteLock};
        }
    }
}