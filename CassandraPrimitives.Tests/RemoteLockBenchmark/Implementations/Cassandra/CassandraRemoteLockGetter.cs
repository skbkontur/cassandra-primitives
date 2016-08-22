using System;

using GroBuf;
using GroBuf.DataMembersExtracters;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock.RemoteLocker;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Implementations.Cassandra
{
    public class CassandraRemoteLockGetter : IRemoteLockGetter, IDisposable
    {
        public CassandraRemoteLockGetter(ICassandraClusterSettings cassandraClusterSettings)
        {
            cassandraCluster = new CassandraCluster(cassandraClusterSettings);

            var serializer = new Serializer(new AllPropertiesExtractor(), null, GroBufOptions.MergeOnRead);
            var implementationSettings = CassandraRemoteLockImplementationSettings.Default(ColumnFamilies.RemoteLock);

            var remoteLockerMetrics = new RemoteLockerMetrics("dummyKeyspace");

            var cassandraRemoteLockImplementation = new CassandraRemoteLockImplementation(cassandraCluster, serializer, implementationSettings);
            remoteLocker = new RemoteLocker(cassandraRemoteLockImplementation, remoteLockerMetrics);
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
    }
}