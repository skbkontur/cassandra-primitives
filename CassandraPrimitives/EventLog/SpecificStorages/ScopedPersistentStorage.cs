using GroBuf;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.PersistentStorages;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.Primitives;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.SpecificStorages
{
    public class ScopedPersistentStorage<T> : PersistentBlobStorage<T, ScopedCassandraObjectId>
        where T : class, IScopedCassandraObject
    {
        public ScopedPersistentStorage(
            ColumnFamilyFullName columnFamilyFullName,
            ICassandraCluster cassandraCluster,
            ISerializer serializer)
            : base(columnFamilyFullName, cassandraCluster, serializer, new ScopedCassandraObjectIdConverter<T>())
        {
        }
    }
}