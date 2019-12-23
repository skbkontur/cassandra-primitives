using System;
using System.Collections.Generic;

using SkbKontur.Cassandra.ThriftClient.Clusters;
using SkbKontur.Cassandra.ThriftClient.Clusters.ActualizationEventListener;
using SkbKontur.Cassandra.ThriftClient.Connections;
using SkbKontur.Cassandra.ThriftClient.Core.Pools;
using SkbKontur.Cassandra.ThriftClient.Scheme;

using SKBKontur.Catalogue.CassandraPrimitives.Storages.Primitives;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.LongWritesConnection
{
    public class CatalogueCassandraClusterWithLongWrites : ICassandraCluster
    {
        public CatalogueCassandraClusterWithLongWrites(ICassandraCluster cluster, TimeSpan columnFamilyConnectionTimeout)
        {
            this.cluster = cluster;
            this.columnFamilyConnectionTimeout = columnFamilyConnectionTimeout;
        }

        public void Dispose()
        {
            cluster.Dispose();
        }

        public IClusterConnection RetrieveClusterConnection()
        {
            return new ClusterConnectionWithLongWrites(cluster.RetrieveClusterConnection());
        }

        public IKeyspaceConnection RetrieveKeyspaceConnection(string keyspace)
        {
            return new KeyspaceConnectionWithLongWrites(cluster.RetrieveKeyspaceConnection(keyspace));
        }

        public IColumnFamilyConnection RetrieveColumnFamilyConnection(string keyspace, string columnFamily)
        {
            return new ColumnFamilyConnectionWithLongWrites(cluster.RetrieveColumnFamilyConnection(keyspace, columnFamily), new ColumnFamilyFullName(keyspace, columnFamily), columnFamilyConnectionTimeout);
        }

        public ITimeBasedColumnFamilyConnection RetrieveTimeBasedColumnFamilyConnection(string keySpaceName, string columnFamilyName)
        {
            throw new NotImplementedException();
        }

        public IColumnFamilyConnectionImplementation RetrieveColumnFamilyConnectionImplementation(string keySpaceName, string columnFamilyName)
        {
            throw new NotImplementedException();
        }

        public Dictionary<ConnectionPoolKey, KeyspaceConnectionPoolKnowledge> GetKnowledges()
        {
            return cluster.GetKnowledges();
        }

        public void ActualizeKeyspaces(KeyspaceScheme[] keyspaces, ICassandraActualizerEventListener eventListener = null, bool changeExistingKeyspaceMetadata = false)
        {
            cluster.ActualizeKeyspaces(keyspaces, eventListener, changeExistingKeyspaceMetadata);
        }

        private readonly ICassandraCluster cluster;
        private readonly TimeSpan columnFamilyConnectionTimeout;
    }
}