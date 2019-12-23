using System.Linq;

using SkbKontur.Cassandra.Primitives.Storages.Primitives;
using SkbKontur.Cassandra.ThriftClient.Abstractions;
using SkbKontur.Cassandra.ThriftClient.Clusters;
using SkbKontur.Cassandra.ThriftClient.Scheme;

namespace CassandraPrimitives.Tests.SchemeActualizer
{
    public class CassandraSchemeActualizer : ICassandraSchemeActualizer
    {
        public CassandraSchemeActualizer(ICassandraCluster cassandraCluster, ICassandraMetadataProvider cassandraMetadataProvider, ICassandraInitializerSettings cassandraInitializerSettings)
        {
            this.cassandraCluster = cassandraCluster;
            this.cassandraMetadataProvider = cassandraMetadataProvider;
            this.cassandraInitializerSettings = cassandraInitializerSettings;
        }

        public void AddNewColumnFamilies()
        {
            var keyspacesFromRegistry = BuildClusterKeyspaces(cassandraMetadataProvider.GetColumnFamilies());
            cassandraCluster.ActualizeKeyspaces(keyspacesFromRegistry);
        }

        public void TruncateAllColumnFamilies()
        {
            var keyspaces = GetKeyspacesFromCassandra();
            foreach (var keyspace in keyspaces)
            {
                foreach (var columnFamily in keyspace.ColumnFamilies.Values)
                    cassandraCluster.RetrieveColumnFamilyConnection(keyspace.Name, columnFamily.Name).Truncate();
            }
        }

        public void TruncateColumnFamily(string keyspace, string columnFamily)
        {
            cassandraCluster.RetrieveColumnFamilyConnection(keyspace, columnFamily).Truncate();
        }

        public void DropDatabase()
        {
            var keyspaces = GetKeyspacesFromCassandra();
            foreach (var keyspace in keyspaces)
            {
                foreach (var columnFamily in keyspace.ColumnFamilies)
                    cassandraCluster.RetrieveColumnFamilyConnection(keyspace.Name, columnFamily.Key).Truncate();
            }
        }

        private Keyspace[] GetKeyspacesFromCassandra()
        {
            return cassandraCluster.RetrieveClusterConnection().RetrieveKeyspaces().ToArray();
        }

        private KeyspaceScheme[] BuildClusterKeyspaces(ColumnFamilyFullName[] columnFamilies)
        {
            var keyspaces = columnFamilies.GroupBy(x => x.KeyspaceName).Select(x => new KeyspaceScheme
                {
                    Name = x.Key,
                    Configuration = new KeyspaceConfiguration
                        {
                            ReplicationStrategy = SimpleReplicationStrategy.Create(cassandraInitializerSettings.ReplicationFactor),
                            ColumnFamilies = x.ToDictionary(y => y.ColumnFamilyName, y => new ColumnFamily
                                {
                                    Name = y.ColumnFamilyName,
                                    Caching = cassandraInitializerSettings.RowCacheSize == 0 ? ColumnFamilyCaching.KeysOnly : ColumnFamilyCaching.All
                                }).Values.ToArray(),
                        },
                }).ToArray();
            return keyspaces;
        }

        private readonly ICassandraCluster cassandraCluster;
        private readonly ICassandraMetadataProvider cassandraMetadataProvider;
        private readonly ICassandraInitializerSettings cassandraInitializerSettings;
    }
}