using GroBuf;

using SkbKontur.Cassandra.Primitives.Storages.PersistentStorages;
using SkbKontur.Cassandra.Primitives.Storages.Primitives;
using SkbKontur.Cassandra.ThriftClient.Clusters;

namespace SkbKontur.Cassandra.Primitives.EventLog.SpecificStorages
{
    internal class ScopedPersistentStorage<T> : PersistentBlobStorage<T, ScopedCassandraObjectId>
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