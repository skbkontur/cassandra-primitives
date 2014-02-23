using SKBKontur.Catalogue.CassandraPrimitives.SchemeActualizer;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLoggerBenchmark.Settings
{
    public class CassandraInitializerSettings : ICassandraInitializerSettings
    {
        public int RowCacheSize { get { return 0; } }
        public int ReplicationFactor { get { return 1; } }
    }
}