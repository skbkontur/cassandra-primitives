﻿using System;

using GroBuf;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Configuration.ColumnFamilies;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Configuration.TypeIdentifiers;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Implementation;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Profiling;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Sharding;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.GlobalTicksHolder;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog
{
    public class EventRepositoryFactory : IEventRepositoryFactory
    {
        public EventRepositoryFactory(
            ISerializer serializer,
            ICassandraCluster cassandraCluster,
            IEventTypeIdentifierProvider eventTypeIdentifierProvider)
        {
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
            var eventInfoRepository = new EventInfoRepository(columnFamilies.EventMeta, cassandraCluster, serializer);
            var eventLogPointerCreator = new EventLogPointerCreator();
            var globalTime = new GlobalTime(ticksHolder);

            var remoteLockImplementation = new CassandraRemoteLockImplementation(cassandraCluster, serializer, CassandraRemoteLockImplementationSettings.Default(columnFamilies.RemoteLock));
            var remoteLockCreator = new RemoteLockCreator(remoteLockImplementation);
            var eventLoggerAdditionalInfoRepository = new EventLoggerAdditionalInfoRepository(cassandraCluster, serializer, remoteLockCreator, columnFamilies.EventLogAdditionalInfo, columnFamilies.EventLog);
            var eventStorage = new EventStorage(columnFamilies.EventLog, eventLogPointerCreator, cassandraCluster, serializer);
            Func<IQueueRaker> createQueueRaker = () => new QueueRaker(eventStorage, eventLoggerAdditionalInfoRepository, eventInfoRepository, profiler);
            var eventLogger = new EventLogger(cassandraCluster, serializer, columnFamilies.EventLog, eventInfoRepository, eventLogPointerCreator, createQueueRaker, eventLoggerAdditionalInfoRepository, globalTime, profiler);
            return new EventRepository(eventTypeIdentifierProvider, eventLogger, shardCalculator, serializer);
        }

        private readonly ISerializer serializer;
        private readonly ICassandraCluster cassandraCluster;
        private readonly IEventTypeIdentifierProvider eventTypeIdentifierProvider;
    }
}