using System;
using System.Net;

using Cassandra;

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

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog
{
    public class EventRepositoryFactory : IEventRepositoryFactory
    {
        public EventRepositoryFactory(
            IPEndPoint[] endpoints,
            ISerializer serializer,
            ICassandraCluster cassandraCluster,
            IEventTypeIdentifierProvider eventTypeIdentifierProvider)
        {
            this.endpoints = endpoints;
            this.serializer = serializer;
            this.cassandraCluster = cassandraCluster;
            this.eventTypeIdentifierProvider = eventTypeIdentifierProvider;
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
            
            var remoteLockImplementation = new CassandraRemoteLockImplementation(endpoints, 9343, CassandraRemoteLockImplementationSettings.Default(columnFamilies.RemoteLock));
            var remoteLocker = new RemoteLocker(remoteLockImplementation, new RemoteLockerMetrics(columnFamilies.RemoteLock.KeyspaceName));
            var eventLoggerAdditionalInfoRepository = new EventLoggerAdditionalInfoRepository(cassandraCluster, serializer, remoteLocker, columnFamilies.EventLogAdditionalInfo, columnFamilies.EventLog);
            var eventStorage = new EventStorage(columnFamilies.EventLog, eventLogPointerCreator, cassandraCluster, serializer);
            Func<IQueueRaker> createQueueRaker = () => new QueueRaker(eventStorage, eventLoggerAdditionalInfoRepository, profiler);
            var eventLogger = new EventLogger(cassandraCluster, serializer, columnFamilies.EventLog, eventLogPointerCreator, createQueueRaker, eventLoggerAdditionalInfoRepository, globalTime, profiler);
            return new EventRepository(eventTypeIdentifierProvider, eventLogger, shardCalculator, serializer);
        }

        private readonly IPEndPoint[] endpoints;
        private readonly ISerializer serializer;
        private readonly ICassandraCluster cassandraCluster;
        private readonly IEventTypeIdentifierProvider eventTypeIdentifierProvider;
    }
}