using System;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.CassandraInitialisation;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.RemoteTaskRunning;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Settings;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.SchemeActualizer;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.CassandraRemoteLock
{
    public class CassandraClusterStarter : IDisposable
    {
        public CassandraClusterStarter()
        {
            ClusterSettings = new CassandraClusterSettings();
            var credentials = new RemoteTaskSchedulerCredentials("K1606012");
            var workDir = AppDomain.CurrentDomain.BaseDirectory;
            cassandraInitializer = new RemoteCassandraInitializer(credentials, workDir);
            try
            {
                cassandraInitializer.CreateNode(new CassandraNodeSettings
                    {
                        ClusterName = ClusterSettings.ClusterName,
                        ListenAddress = "127.0.0.1",
                        SeedAddresses = new[] {"127.0.0.1"}
                    });
                var initializerSettings = new CassandraInitializerSettings();
                using (var cassandraCluster = new CassandraCluster(ClusterSettings))
                {
                    var cassandraSchemeActualizer = new CassandraSchemeActualizer(cassandraCluster, new CassandraMetaProvider(), initializerSettings);
                    cassandraSchemeActualizer.AddNewColumnFamilies();
                }
            }
            catch (Exception)
            {
                cassandraInitializer.Dispose();
                throw;
            }
        }

        public ICassandraClusterSettings ClusterSettings { get; private set; }

        public void Dispose()
        {
            cassandraInitializer.Dispose();
        }

        private readonly ICassandraInitialiser cassandraInitializer;
    }
}