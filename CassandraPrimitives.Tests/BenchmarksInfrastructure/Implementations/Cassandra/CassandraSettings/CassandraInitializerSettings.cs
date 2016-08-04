using SKBKontur.Catalogue.CassandraPrimitives.Tests.SchemeActualizer;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Implementations.Cassandra.CassandraSettings
{
    public class CassandraInitializerSettings : ICassandraInitializerSettings
    {
        public CassandraInitializerSettings(int rowCacheSize = 0, int replicationFactor = 1)
        {
            RowCacheSize = rowCacheSize;
            ReplicationFactor = replicationFactor;
        }

        public int RowCacheSize { get; private set; }
        public int ReplicationFactor { get; private set; }
    }
}