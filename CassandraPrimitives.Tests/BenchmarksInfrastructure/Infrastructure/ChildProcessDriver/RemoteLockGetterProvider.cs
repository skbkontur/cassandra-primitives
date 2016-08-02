using System;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Implementations.RemoteLocks;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Implementations.RemoteLocks.Cassandra;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Implementations.RemoteLocks.Zookeeper;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.ExternalLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.ExternalLogging.HttpLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.TestConfigurations;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.ChildProcessDriver
{
    public class RemoteLockGetterProvider : IRemoteLockGetterProvider
    {
        public RemoteLockGetterProvider(HttpExternalDataGetter httpExternalDataGetter, TestConfiguration configuration, IExternalLogger externalLogger)
        {
            switch (configuration.ClusterType)
            {
            case ClusterTypes.Cassandra:
                var cassandraClusterSettings = httpExternalDataGetter.GetCassandraSettings().Result;
                getter = () => new CassandraRemoteLockGetter(cassandraClusterSettings);
                break;
            case ClusterTypes.Zookeeper:
                var zookeeperClusterSettings = httpExternalDataGetter.GetZookeeperSettings().Result;
                getter = () => new ZookeeperRemoteLockGetter(new ZookeeperLockSettings(zookeeperClusterSettings.ConnectionString, "/RemoteLockBenchmark", TimeSpan.FromSeconds(100)));
                break;
            default:
                throw new Exception(string.Format("Unknown remote lock implementation {0}", configuration.ClusterType));
            }
        }

        public IRemoteLockGetter GetRemoteLockGetter()
        {
            return getter();
        }

        private readonly Func<IRemoteLockGetter> getter;
    }
}