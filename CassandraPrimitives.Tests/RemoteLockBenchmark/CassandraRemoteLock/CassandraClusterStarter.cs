using System;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.ClusterDeployment;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Settings;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.SchemeActualizer;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.CassandraRemoteLock
{
    public class CassandraClusterStarter : IDisposable
    {
        public CassandraClusterStarter()
        {
            ClusterSettings = new CassandraClusterSettings();
            node = CassandraInitializer.CreateCassandraNode(ClusterSettings.ClusterName, "127.0.0.1", new[] {"127.0.0.1"});
            node.Restart();

            var initializerSettings = new CassandraInitializerSettings();
            using (var cassandraCluster = new CassandraCluster(ClusterSettings))
            {
                var cassandraSchemeActualizer = new CassandraSchemeActualizer(cassandraCluster, new CassandraMetaProvider(), initializerSettings);
                cassandraSchemeActualizer.AddNewColumnFamilies();
            }
        }

        public ICassandraClusterSettings ClusterSettings { get; private set; }

        public void Dispose()
        {
            node.Stop();
        }

        private readonly CassandraNode node;
    }
}