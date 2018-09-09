using System;

using GroBuf;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Configuration.ColumnFamilies;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Configuration.TypeIdentifiers;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Implementation;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Profiling;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Sharding;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock.RemoteLocker;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.GlobalTicksHolder;

using Vostok.Logging.Abstractions;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog
{
    public class EventRepositoryFactory : IEventRepositoryFactory
    {
        public EventRepositoryFactory(
            ISerializer serializer,
            ICassandraCluster cassandraCluster,
            IEventTypeIdentifierProvider eventTypeIdentifierProvider,
            ILog logger
            )
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

        public IEventRepository CreateEventRepository(IShardCalculator shardCalculator, IEventRepositoryColumnFamilyFullNames columnFamilies, IEventLogProfiler profiler)
        {
            var ticksHolder = new TicksHolder(serializer, cassandraCluster, columnFamilies.TicksHolder);
            var eventLogPointerCreator = new EventLogPointerCreator();
            var globalTime = new GlobalTime(ticksHolder);

            var remoteLockImplementation = new CassandraRemoteLockImplementation(cassandraCluster, serializer, CassandraRemoteLockImplementationSettings.Default(columnFamilies.RemoteLock.KeyspaceName, columnFamilies.RemoteLock.ColumnFamilyName));
            var remoteLocker = new RemoteLocker(remoteLockImplementation, new RemoteLockerMetrics(columnFamilies.RemoteLock.KeyspaceName), logger);
            var eventLoggerAdditionalInfoRepository = new EventLoggerAdditionalInfoRepository(cassandraCluster, serializer, remoteLocker, columnFamilies.EventLogAdditionalInfo, columnFamilies.EventLog);
            var eventStorage = new EventStorage(columnFamilies.EventLog, eventLogPointerCreator, cassandraCluster, serializer);
            Func<IQueueRaker> createQueueRaker = () => new QueueRaker(eventStorage, eventLoggerAdditionalInfoRepository, profiler, logger);
            var eventLogger = new EventLogger(cassandraCluster, serializer, columnFamilies.EventLog, eventLogPointerCreator, createQueueRaker, eventLoggerAdditionalInfoRepository, globalTime, profiler, logger);
            return new EventRepository(eventTypeIdentifierProvider, eventLogger, shardCalculator, serializer);
        }

        private readonly ISerializer serializer;
        private readonly ICassandraCluster cassandraCluster;
        private readonly IEventTypeIdentifierProvider eventTypeIdentifierProvider;
        private readonly ILog logger;
    }
}