using System;

using GroBuf;
using GroBuf.DataMembersExtracters;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock.RemoteLocker;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.Commons.Logging;

using Vostok.Logging;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Implementations.Cassandra
{
    public class CassandraRemoteLockGetter : IRemoteLockGetter, IDisposable
    {
        public CassandraRemoteLockGetter(ICassandraClusterSettings cassandraClusterSettings)
        {
            cassandraCluster = new CassandraCluster(cassandraClusterSettings, logger);

            var serializer = new Serializer(new AllPropertiesExtractor(), null, GroBufOptions.MergeOnRead);
            var implementationSettings = CassandraRemoteLockImplementationSettings.Default(ColumnFamilies.RemoteLock.KeyspaceName, ColumnFamilies.RemoteLock.ColumnFamilyName);

            var remoteLockerMetrics = new RemoteLockerMetrics(ColumnFamilies.RemoteLock.KeyspaceName);

            var cassandraRemoteLockImplementation = new CassandraRemoteLockImplementation(cassandraCluster, serializer, implementationSettings);
            remoteLocker = new RemoteLocker(cassandraRemoteLockImplementation, remoteLockerMetrics, logger);
        }

        public IRemoteLock Get(string lockId)
        {
            return new CassandraRemoteLock(remoteLocker, lockId);
        }

        public void Dispose()
        {
            remoteLocker.Dispose();
            cassandraCluster.Dispose();
        }

        private readonly ICassandraCluster cassandraCluster;
        private readonly RemoteLocker remoteLocker;
        private static readonly ILog logger = new Log4NetWrapper(typeof(CassandraRemoteLockGetter));
    }
}