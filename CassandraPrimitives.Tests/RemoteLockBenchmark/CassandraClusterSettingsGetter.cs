using System.IO;

using SKBKontur.Cassandra.CassandraClient.Clusters;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark
{
    public class CassandraClusterSettingsGetter
    {
        public static ICassandraClusterSettings GetSettings()
        {
            return ImprovedJsonSerializer.Deserialize<CassandraClusterSettings>(File.ReadAllText("cassandra_settings.txt"));
        }
    }
}