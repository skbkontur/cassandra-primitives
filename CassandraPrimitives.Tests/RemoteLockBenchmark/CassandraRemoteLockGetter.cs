using System;
using System.Collections.Generic;

using GroBuf;
using GroBuf.DataMembersExtracters;

using log4net;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock.RemoteLocker;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Settings;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark
{
    public class CassandraRemoteLockGetter : IRemoteLockGetter, IDisposable
    {
        private readonly List<RemoteLocker> remoteLockersToDispose;
        private readonly ILog logger;
        private readonly Action<string> externalLogger;
        
        private readonly TimeSpan lockTtl;
        private readonly TimeSpan keepLockAliveInterval;
        private readonly int changeLockRowThreshold;
        private readonly ICassandraCluster cassandraCluster;

        public CassandraRemoteLockGetter(ICassandraClusterSettings cassandraClusterSettings, Action<string> externalLogger)
        {
            remoteLockersToDispose = new List<RemoteLocker>();
            logger = LogManager.GetLogger(GetType());
            this.externalLogger = externalLogger;

            cassandraCluster = new CassandraCluster(cassandraClusterSettings);

            lockTtl = TimeSpan.FromSeconds(10);
            keepLockAliveInterval = TimeSpan.FromSeconds(2);
            changeLockRowThreshold = 10;
        }

        public IRemoteLockCreator[] Get(int amount)
        {
            var serializer = new Serializer(new AllPropertiesExtractor(), null, GroBufOptions.MergeOnRead);
            var timestampProvider = new DefaultTimestampProvider();
            var implementationSettings = new CassandraRemoteLockImplementationSettings(timestampProvider, ColumnFamilies.remoteLock, lockTtl, keepLockAliveInterval, changeLockRowThreshold);

            var remoteLockerMetrics = new RemoteLockerMetrics("dummyKeyspace");

            var remoteLockers = new IRemoteLockCreator[amount];
            for (int i = 0; i < amount; i++)
            {
                var cassandraRemoteLockImplementation = new CassandraRemoteLockImplementation(cassandraCluster, serializer, implementationSettings);
                var remoteLocker = new RemoteLocker(cassandraRemoteLockImplementation, remoteLockerMetrics);
                remoteLockers[i] = remoteLocker;
                remoteLockersToDispose.Add(remoteLocker);
            }

            return remoteLockers;
        }

        public void Dispose()
        {
            foreach(var remoteLocker in remoteLockersToDispose)
            {
                try
                {
                    remoteLocker.Dispose();
                }
                catch(Exception e)
                {
                    logger.Error("Exception occured while disposing remoteLocker:", e);
                    externalLogger(String.Format("Exception occured while disposing remoteLocker:\n{0}", e));
                }
            }
            cassandraCluster.Dispose();
        }
    }
}