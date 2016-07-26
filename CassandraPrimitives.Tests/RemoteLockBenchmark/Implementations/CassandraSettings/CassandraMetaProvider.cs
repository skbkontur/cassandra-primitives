using SKBKontur.Catalogue.CassandraPrimitives.Storages.Primitives;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.SchemeActualizer;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Implementations.CassandraSettings
{
    public class CassandraMetaProvider : ICassandraMetadataProvider
    {
        public ColumnFamilyFullName[] GetColumnFamilies()
        {
            return new[] {ColumnFamilies.remoteLock};
        }
    }
}