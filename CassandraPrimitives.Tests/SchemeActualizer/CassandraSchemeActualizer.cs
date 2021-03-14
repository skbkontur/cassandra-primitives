using System.Linq;

using SkbKontur.Cassandra.ThriftClient.Abstractions;
using SkbKontur.Cassandra.ThriftClient.Clusters;
using SkbKontur.Cassandra.ThriftClient.Schema;

namespace CassandraPrimitives.Tests.SchemeActualizer
{
    public class CassandraSchemeActualizer
    {
        public CassandraSchemeActualizer(ICassandraCluster cassandraCluster, ICassandraMetadataProvider cassandraMetadataProvider, ICassandraInitializerSettings cassandraInitializerSettings)
        {
            this.cassandraCluster = cassandraCluster;
            this.cassandraMetadataProvider = cassandraMetadataProvider;
            this.cassandraInitializerSettings = cassandraInitializerSettings;
            cassandraSchemaActualizer = new CassandraSchemaActualizer(cassandraCluster, eventListener : null, Logger.Instance);
        }

        public void AddNewColumnFamilies()
        {
            var keyspaceShemas = cassandraMetadataProvider.GetColumnFamilies().GroupBy(x => x.KeyspaceName).Select(x => new KeyspaceSchema
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
            cassandraSchemaActualizer.ActualizeKeyspaces(keyspaceShemas, changeExistingKeyspaceMetadata : false);
        }

        public void TruncateAllColumnFamilies()
        {
            var keyspaces = cassandraCluster.RetrieveClusterConnection().RetrieveKeyspaces().ToArray();
            foreach (var keyspace in keyspaces)
            {
                foreach (var columnFamily in keyspace.ColumnFamilies.Values)
                    cassandraCluster.RetrieveColumnFamilyConnection(keyspace.Name, columnFamily.Name).Truncate();
            }
        }

        private readonly ICassandraCluster cassandraCluster;
        private readonly ICassandraMetadataProvider cassandraMetadataProvider;
        private readonly ICassandraInitializerSettings cassandraInitializerSettings;
        private readonly CassandraSchemaActualizer cassandraSchemaActualizer;
    }
}