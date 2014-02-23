using System;

using GroBuf;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Configuration.ColumnFamilies;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Configuration.TypeIdentifiers;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Implementation;
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
            ICassandraClusterSettings cassandraClusterSettings,
            IEventTypeIdentifierProvider eventTypeIdentifierProvider)
        {
            this.serializer = serializer;
            this.cassandraCluster = cassandraCluster;
            this.cassandraClusterSettings = cassandraClusterSettings;
            this.eventTypeIdentifierProvider = eventTypeIdentifierProvider;
        }

        public IEventRepository CreateEventRepository(
            IShardCalculator shardCalculator,
            IEventRepositoryColumnFamilyFullNames columnFamilies)
        {
            var ticksHolder = new TicksHolder(serializer, cassandraCluster, columnFamilies.TicksHolder);
            var eventInfoRepository = new EventInfoRepository(columnFamilies.EventMeta, cassandraCluster, serializer);
            var eventLogPointerCreator = new EventLogPointerCreator();
            var globalTime = new GlobalTime(ticksHolder);

            var remoteLockCreator = new RemoteLockCreator(new CassandraRemoteLockImplementation(cassandraCluster, cassandraClusterSettings, serializer, columnFamilies.RemoteLock));
            var eventLoggerAdditionalInfoRepository = new EventLoggerAdditionalInfoRepository(cassandraCluster, serializer, remoteLockCreator, columnFamilies.EventLogAdditionalInfo, columnFamilies.EventLog);
            var eventStorage = new EventStorage(columnFamilies.EventLog, eventLogPointerCreator, cassandraCluster, serializer);
            Func<IQueueRaker> createQueueRaker = () => new QueueRaker(eventStorage, eventLoggerAdditionalInfoRepository, eventInfoRepository);
            return new EventRepository(
                eventTypeIdentifierProvider,
                new EventLogger(cassandraCluster, serializer, columnFamilies.EventLog, eventInfoRepository, eventLogPointerCreator, createQueueRaker, eventLoggerAdditionalInfoRepository, globalTime),
                shardCalculator,
                serializer);
        }

        private readonly ISerializer serializer;
        private readonly ICassandraCluster cassandraCluster;
        private readonly ICassandraClusterSettings cassandraClusterSettings;
        private readonly IEventTypeIdentifierProvider eventTypeIdentifierProvider;
    }
}