using System;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Implementations.RemoteLocks;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Implementations.RemoteLocks.Cassandra;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Implementations.RemoteLocks.Zookeeper;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.ExternalLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.ExternalLogging.HttpLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.TestConfigurations;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.ChildProcessDriver
{
    public class RemoteLockGetterProvider : IRemoteLockGetterProvider
    {
        public RemoteLockGetterProvider(HttpExternalDataGetter httpExternalDataGetter, TestConfiguration configuration, IExternalLogger externalLogger)
        {
            switch (configuration.RemoteLockImplementation)
            {
            case RemoteLockImplementations.Cassandra:
                var cassandraClusterSettings = httpExternalDataGetter.GetCassandraSettings().Result;
                getter = () => new CassandraRemoteLockGetter(cassandraClusterSettings);
                break;
            case RemoteLockImplementations.Zookeeper:
                var zookeeperClusterSettings = httpExternalDataGetter.GetZookeeperSettings().Result;
                getter = () => new ZookeeperRemoteLockGetter(new ZookeeperLockSettings(zookeeperClusterSettings.ConnectionString, "/RemoteLockBenchmark", TimeSpan.FromSeconds(100)));
                break;
            default:
                throw new Exception(string.Format("Unknown remote lock implementation {0}", configuration.RemoteLockImplementation));
            }
        }

        public IRemoteLockGetter GetRemoteLockGetter()
        {
            return getter();
        }

        private readonly Func<IRemoteLockGetter> getter;
    }
}