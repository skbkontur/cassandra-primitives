using System;
using System.Linq;
using System.Threading.Tasks;

using GroBuf;
using GroBuf.DataMembersExtracters;

using Metrics;
using Metrics.Reporters;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock.RemoteLocker;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.Commons.Logging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Settings;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Tests.RemoteLockTests.FiledCassandra;

using Vostok.Logging;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Tests.RemoteLockTests
{
    public class RemoteLockerTester : IDisposable, IRemoteLockCreator
    {
        public RemoteLockerTester(RemoteLockerTesterConfig config = null)
        {
            config = config ?? RemoteLockerTesterConfig.Default();
            var localRivalOptimizationIsEnabled = config.LocalRivalOptimization != LocalRivalOptimization.Disabled;
            var serializer = new Serializer(new AllPropertiesExtractor(), null, GroBufOptions.MergeOnRead);
            ICassandraCluster cassandraCluster = new CassandraCluster(config.CassandraClusterSettings, logger);
            if(config.CassandraFailProbability.HasValue)
                cassandraCluster = new FailedCassandraCluster(cassandraCluster, config.CassandraFailProbability.Value);
            var timestampProvider = new StochasticTimestampProvider(config.TimestamProviderStochasticType, config.LockTtl);
            var implementationSettings = new CassandraRemoteLockImplementationSettings(timestampProvider, ColumnFamilies.remoteLock, config.LockTtl, config.LockMetadataTtl, config.KeepLockAliveInterval, config.ChangeLockRowThreshold);
            var cassandraRemoteLockImplementation = new CassandraRemoteLockImplementation(cassandraCluster, serializer, implementationSettings);
            remoteLockers = new RemoteLocker[config.LockersCount];
            remoteLockerMetrics = new RemoteLockerMetrics("dummyKeyspace");
            if(localRivalOptimizationIsEnabled)
            {
                var remoteLocker = new RemoteLocker(cassandraRemoteLockImplementation, remoteLockerMetrics, logger);
                for(var i = 0; i < config.LockersCount; i++)
                    remoteLockers[i] = remoteLocker;
            }
            else
            {
                for(var i = 0; i < config.LockersCount; i++)
                    remoteLockers[i] = new RemoteLocker(new CassandraRemoteLockImplementation(cassandraCluster, serializer, implementationSettings), remoteLockerMetrics, logger);
            }
            // it is important to use another CassandraCluster (with another setting of attempts, for example)
            cassandraRemoteLockImplementationForCheckings = new CassandraRemoteLockImplementation(new CassandraCluster(CassandraClusterSettings.ForNode(SingleCassandraNodeSetUpFixture.Node), logger), serializer, implementationSettings);
        }

        public void Dispose()
        {
            var disposeTasks = new Task[remoteLockers.Length];
            for(var i = 0; i < remoteLockers.Length; i++)
            {
                var localI = i;
                disposeTasks[i] = Task.Factory.StartNew(() => remoteLockers[localI].Dispose());
            }
            Task.WaitAll(disposeTasks);
            LogRemoteLockerPerfStat();
        }

        private void LogRemoteLockerPerfStat()
        {
            var metricsData = remoteLockerMetrics.Context.DataProvider.CurrentMetricsData;
            var metricsReport = StringReport.RenderMetrics(metricsData, () => new HealthStatus());
            Console.Out.WriteLine(metricsReport);
        }

        public IRemoteLockCreator this[int index] { get { return remoteLockers[index]; } }

        public IRemoteLock Lock(string lockId)
        {
            return remoteLockers.Single().Lock(lockId);
        }

        public bool TryGetLock(string lockId, out IRemoteLock remoteLock)
        {
            return remoteLockers.Single().TryGetLock(lockId, out remoteLock);
        }

        public string[] GetThreadsInMainRow(string lockId)
        {
            return cassandraRemoteLockImplementationForCheckings.GetLockThreads(lockId);
        }

        public string[] GetThreadsInShadeRow(string lockId)
        {
            return cassandraRemoteLockImplementationForCheckings.GetShadeThreads(lockId);
        }

        public LockMetadata GetLockMetadata(string lockId)
        {
            return cassandraRemoteLockImplementationForCheckings.GetLockMetadata(lockId);
        }

        private readonly RemoteLocker[] remoteLockers;
        private readonly RemoteLockerMetrics remoteLockerMetrics;
        private readonly CassandraRemoteLockImplementation cassandraRemoteLockImplementationForCheckings;
        private static readonly ILog logger = new Log4NetWrapper(typeof(RemoteLockerTester));
    }
}