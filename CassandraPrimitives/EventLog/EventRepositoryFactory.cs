using System;

using GroBuf;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Catalogue.CassandraPrimitives.CasRemoteLock;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Configuration.ColumnFamilies;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Configuration.TypeIdentifiers;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Implementation;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Profiling;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Sharding;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.GlobalTicksHolder;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog
{
    public class EventRepositoryFactory : IEventRepositoryFactory
    {
        public EventRepositoryFactory(
            ISerializer serializer,
            ICassandraCluster cassandraCluster,
            CasRemoteLockProvider remoteLockProvider,
            IEventTypeIdentifierProvider eventTypeIdentifierProvider)
        {
            this.serializer = serializer;
            this.remoteLockProvider = remoteLockProvider;
            remoteLockProvider.ActualiseTables();
            remoteLockProvider.InitPreparedStatements();
            this.eventTypeIdentifierProvider = eventTypeIdentifierProvider;
            this.cassandraCluster = cassandraCluster;
        }

        public IEventRepository CreateEventRepository(IShardCalculator shardCalculator, IEventRepositoryColumnFamilyFullNames columnFamilies)
        {
            return CreateEventRepository(shardCalculator, columnFamilies, EventLogNullProfiler.Instance);
        }

        public IEventRepository CreateEventRepository(IShardCalculator shardCalculator, IEventRepositoryColumnFamilyFullNames columnFamilies, IEventLogProfiler profiler)
        {
            //throw new Exception("Event repository is temporarly not supported");

            var ticksHolder = new TicksHolder(serializer, cassandraCluster, columnFamilies.TicksHolder);
            var eventLogPointerCreator = new EventLogPointerCreator();
            var globalTime = new GlobalTime(ticksHolder);

            //var remoteLockImplementation = new CassandraRemoteLockImplementation(cassandraCluster, serializer, CassandraRemoteLockImplementationSettings.Default(columnFamilies.RemoteLock));
            //var remoteLocker = new RemoteLocker(remoteLockImplementation, new RemoteLockerMetrics(columnFamilies.RemoteLock.KeyspaceName));

            var remoteLocker = remoteLockProvider.CreateLocker();
            
            var eventLoggerAdditionalInfoRepository = new EventLoggerAdditionalInfoRepository(cassandraCluster, serializer, remoteLocker, columnFamilies.EventLogAdditionalInfo, columnFamilies.EventLog);
            var eventStorage = new EventStorage(columnFamilies.EventLog, eventLogPointerCreator, cassandraCluster, serializer);
            Func<IQueueRaker> createQueueRaker = () => new QueueRaker(eventStorage, eventLoggerAdditionalInfoRepository, profiler);
            var eventLogger = new EventLogger(cassandraCluster, serializer, columnFamilies.EventLog, eventLogPointerCreator, createQueueRaker, eventLoggerAdditionalInfoRepository, globalTime, profiler);
            return new EventRepository(eventTypeIdentifierProvider, eventLogger, shardCalculator, serializer);
        }

        private readonly ISerializer serializer;
        private readonly CasRemoteLockProvider remoteLockProvider;
        private readonly ICassandraCluster cassandraCluster;
        private readonly IEventTypeIdentifierProvider eventTypeIdentifierProvider;
    }
}