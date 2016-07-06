using System;
using System.Collections.Generic;
using System.Net;

using GroBuf;
using GroBuf.DataMembersExtracters;

using log4net;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.ClusterDeployment;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock.RemoteLocker;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Settings;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.SchemeActualizer;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark
{
    public class CassandraRemoteLockGetter : IRemoteLockGetter, IDisposable
    {
        private readonly CassandraNode node;
        private readonly ICassandraClusterSettings cassandraClusterSettings;
        private readonly List<RemoteLocker> remoteLockersToDispose;
        private readonly ILog logger;
        private readonly ITeamCityLogger teamCityLogger;

        public CassandraRemoteLockGetter(ITeamCityLogger teamCityLogger)
        {
            node = CassandraInitializer.CreateCassandraNode();
            node.Restart();
            cassandraClusterSettings = node.CreateSettings(IPAddress.Loopback);
            remoteLockersToDispose = new List<RemoteLocker>();
            logger = LogManager.GetLogger(GetType());
            this.teamCityLogger = teamCityLogger;
        }
        public IRemoteLockCreator[] Get(int amount)
        {
            var initializerSettings = new CassandraInitializerSettings();
            ICassandraCluster cassandraCluster = new CassandraCluster(cassandraClusterSettings);
            var cassandraSchemeActualizer = new CassandraSchemeActualizer(cassandraCluster, new CassandraMetaProvider(), initializerSettings);
            cassandraSchemeActualizer.AddNewColumnFamilies();

            var lockTtl = TimeSpan.FromSeconds(10);
            var keepLockAliveInterval = TimeSpan.FromSeconds(2);
            var changeLockRowThreshold = 10;

            var remoteLockers = GetRemoteLockers(cassandraCluster, lockTtl, keepLockAliveInterval, changeLockRowThreshold, amount);

            return remoteLockers;
        }

        private IRemoteLockCreator[] GetRemoteLockers(ICassandraCluster cassandraCluster, TimeSpan lockTtl, TimeSpan keepLockAliveInterval, int changeLockRowThreshold, int amount)
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
                    teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Failure, "Exception occured while disposing remoteLocker:\n{0}", e);
                }
            }
            node.Stop();
        }
    }
}