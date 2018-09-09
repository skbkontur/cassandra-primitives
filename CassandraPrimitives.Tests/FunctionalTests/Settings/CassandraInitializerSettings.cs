using SKBKontur.Catalogue.CassandraPrimitives.Tests.SchemeActualizer;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Settings
{
    public class CassandraInitializerSettings : ICassandraInitializerSettings
    {
        public int RowCacheSize { get { return 0; } }
        public int ReplicationFactor { get { return 1; } }
    }
}