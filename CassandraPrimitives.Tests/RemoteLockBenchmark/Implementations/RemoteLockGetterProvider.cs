using System;
using System.Threading;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.ExternalLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.ExternalLogging.HttpLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.TestConfigurations;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Implementations.Cassandra;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Implementations.CASCassandra;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Implementations.Zookeeper;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Implementations
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
                case ClusterTypes.CassandraCas:
                    cassandraClusterSettings = httpExternalDataGetter.GetCassandraSettings().Result;
                    getter = () =>
                    {
                        lock (cassandraClusterSettings)
                        {
                            if (cassandraCasRemoteLockGetter == null)
                                cassandraCasRemoteLockGetter = new CassandraCasRemoteLockGetter(cassandraClusterSettings);
                        }
                        return cassandraCasRemoteLockGetter;
                    };
                    break;
                case ClusterTypes.Zookeeper:
                    var zookeeperClusterSettings = httpExternalDataGetter.GetZookeeperSettings().Result;
                    getter = () => new ZookeeperRemoteLockGetter(new ZookeeperLockSettings(zookeeperClusterSettings.ConnectionString, "/RemoteLockBenchmark", TimeSpan.FromSeconds(100)));
                    break;
                default:
                    throw new Exception(string.Format("Unknown remote lock implementation {0}", configuration.ClusterType));

            }
        }

        private CassandraCasRemoteLockGetter cassandraCasRemoteLockGetter;

        public IRemoteLockGetter GetRemoteLockGetter()
        {
            return getter();
        }

        private readonly Func<IRemoteLockGetter> getter;
    }
}