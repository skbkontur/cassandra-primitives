using System;
using System.Net;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.ClusterDeployment;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Settings;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.SchemeActualizer;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.CassandraRemoteLock
{
    public class CassandraServerStarter : IDisposable
    {
        public CassandraServerStarter()
        {
            node = CassandraInitializer.CreateCassandraNode();
            node.Restart();
            ClusterSettings = node.CreateSettings(IPAddress.Loopback);

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