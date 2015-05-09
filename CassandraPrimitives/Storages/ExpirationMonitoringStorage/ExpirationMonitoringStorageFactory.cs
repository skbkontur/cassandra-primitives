using GroBuf;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.Primitives;

namespace SKBKontur.Catalogue.CassandraPrimitives.Storages.ExpirationMonitoringStorage
{
    public class ExpirationMonitoringStorageFactory : IExpirationMonitoringStorageFactory
    {
        public ExpirationMonitoringStorageFactory(ICassandraCluster cassandraCluster, ISerializer serializer)
        {
            this.cassandraCluster = cassandraCluster;
            this.serializer = serializer;
        }

        public IExpirationMonitoringStorage CreateStorage(ColumnFamilyFullName columnFamilyFullName)
        {
            return new ExpirationMonitoringStorage(cassandraCluster, serializer, columnFamilyFullName);
        }

        private readonly ICassandraCluster cassandraCluster;
        private readonly ISerializer serializer;
    }
}