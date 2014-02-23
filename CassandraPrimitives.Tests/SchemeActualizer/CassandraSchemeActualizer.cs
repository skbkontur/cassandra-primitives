using System.Collections.Generic;
using System.Linq;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;

namespace SKBKontur.Catalogue.CassandraPrimitives.SchemeActualizer
{
    public interface ICassandraMetadataProvider
    {
        IEnumerable<Keyspace> BuildClusterKeyspaces(ICassandraInitializerSettings cassandraInitializerSettings);
    }

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
            var keyspacesFromCassandra = GetKeyspacesFromCassandra().ToDictionary(x => x.Name);
            var keyspacesFromRegistry = GetKeyspacesFromRegistry().ToDictionary(x => x.Name);

            foreach (var keyspaceName in keyspacesFromRegistry.Keys)
            {
                var keyspace = keyspacesFromRegistry[keyspaceName];
                if (!keyspacesFromCassandra.ContainsKey(keyspaceName))
                    cassandraCluster.RetrieveClusterConnection().AddKeyspace(keyspace);
                else
                {
                    var columnFamilies = keyspace.ColumnFamilies;
                    keyspace.ColumnFamilies = null;
                    cassandraCluster.RetrieveClusterConnection().UpdateKeyspace(keyspace);
                    keyspace.ColumnFamilies = columnFamilies;
                }
            }

            foreach (var keyspaceName in keyspacesFromCassandra.Keys.Where(keyspacesFromRegistry.ContainsKey))
            {
                var keyspaceFromCassandra = keyspacesFromCassandra[keyspaceName];
                var keyspaceFromRegistry = keyspacesFromRegistry[keyspaceName];
                foreach (var columnFamily in keyspaceFromRegistry.ColumnFamilies.Values)
                {
                    if (!keyspaceFromCassandra.ColumnFamilies.ContainsKey(columnFamily.Name))
                        cassandraCluster.RetrieveKeyspaceConnection(keyspaceName).AddColumnFamily(columnFamily);
                }
            }
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

        private Keyspace[] GetKeyspacesFromRegistry()
        {
            return cassandraMetadataProvider.BuildClusterKeyspaces(cassandraInitializerSettings).ToArray();
        }

        private readonly ICassandraCluster cassandraCluster;
        private readonly ICassandraMetadataProvider cassandraMetadataProvider;
        private readonly ICassandraInitializerSettings cassandraInitializerSettings;
    }
}