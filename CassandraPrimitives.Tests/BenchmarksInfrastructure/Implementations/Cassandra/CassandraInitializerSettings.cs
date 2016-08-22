using SKBKontur.Catalogue.CassandraPrimitives.Tests.SchemeActualizer;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Implementations.Cassandra
{
    public class CassandraInitializerSettings : ICassandraInitializerSettings
    {
        public CassandraInitializerSettings(int rowCacheSize, int replicationFactor)
        {
            RowCacheSize = rowCacheSize;
            ReplicationFactor = replicationFactor;
        }

        public int RowCacheSize { get; private set; }
        public int ReplicationFactor { get; private set; }
    }
}