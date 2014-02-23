using GroBuf;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Primitives;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.SpecificStorages;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.PersistentStorages;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.Primitives;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.Implementation
{
    internal class EventInfoRepository : PersistentBlobStorage<EventInfo, EventId>, IEventInfoRepository
    {
        public EventInfoRepository(ColumnFamilyFullName columnFamilyFullName, ICassandraCluster cassandraCluster, ISerializer serializer)
            : base(columnFamilyFullName, cassandraCluster, serializer, new EventIdConverter())
        {
        }
    }
}