using System;

using GroBuf;

using SkbKontur.Cassandra.DistributedLock;
using SkbKontur.Cassandra.DistributedLock.RemoteLocker;
using SkbKontur.Cassandra.Primitives.EventLog.Configuration.ColumnFamilies;
using SkbKontur.Cassandra.Primitives.EventLog.Configuration.TypeIdentifiers;
using SkbKontur.Cassandra.Primitives.EventLog.Implementation;
using SkbKontur.Cassandra.Primitives.EventLog.Profiling;
using SkbKontur.Cassandra.Primitives.EventLog.Sharding;
using SkbKontur.Cassandra.ThriftClient.Clusters;

using Vostok.Logging.Abstractions;

namespace SkbKontur.Cassandra.Primitives.EventLog
{
    public class EventRepositoryFactory : IEventRepositoryFactory
    {
        public EventRepositoryFactory(
            ISerializer serializer,
            ICassandraCluster cassandraCluster,
            IEventTypeIdentifierProvider eventTypeIdentifierProvider,
            ILog logger)
        {
            this.serializer = serializer;
            this.cassandraCluster = cassandraCluster;
            this.eventTypeIdentifierProvider = eventTypeIdentifierProvider;
            this.logger = logger.ForContext("CassandraPrimitives.EventLog");
        }

        public IEventRepository CreateEventRepository(IShardCalculator shardCalculator, IEventRepositoryColumnFamilyFullNames columnFamilies)
        {
            return CreateEventRepository(shardCalculator, columnFamilies, EventLogNullProfiler.Instance);
        }

        public IEventRepository CreateEventRepository(IShardCalculator shardCalculator, IEventRepositoryColumnFamilyFullNames columnFamilies, IEventLogProfiler profiler, TimeSpan? eventsTtl = null)
        {
            var eventLogPointerCreator = new EventLogPointerCreator();

            var remoteLockImplementation = new CassandraRemoteLockImplementation(cassandraCluster, serializer, CassandraRemoteLockImplementationSettings.Default(columnFamilies.RemoteLock.KeyspaceName, columnFamilies.RemoteLock.ColumnFamilyName));
            var remoteLocker = new RemoteLocker(remoteLockImplementation, new RemoteLockerMetrics(columnFamilies.RemoteLock.KeyspaceName), logger);
            var eventLoggerAdditionalInfoRepository = new EventLoggerAdditionalInfoRepository(cassandraCluster, serializer, remoteLocker, columnFamilies.EventLogAdditionalInfo, columnFamilies.EventLog);
            var eventStorage = new EventStorage(columnFamilies.EventLog, eventLogPointerCreator, cassandraCluster, serializer);
            Func<IQueueRaker> createQueueRaker = () => new QueueRaker(eventStorage, eventLoggerAdditionalInfoRepository, profiler, logger, eventsTtl);
            var eventLogger = new EventLogger(cassandraCluster, serializer, columnFamilies.EventLog, eventLogPointerCreator, createQueueRaker, eventLoggerAdditionalInfoRepository, profiler, logger);
            return new EventRepository(eventTypeIdentifierProvider, eventLogger, shardCalculator, serializer);
        }

        private readonly ISerializer serializer;
        private readonly ICassandraCluster cassandraCluster;
        private readonly IEventTypeIdentifierProvider eventTypeIdentifierProvider;
        private readonly ILog logger;
    }
}