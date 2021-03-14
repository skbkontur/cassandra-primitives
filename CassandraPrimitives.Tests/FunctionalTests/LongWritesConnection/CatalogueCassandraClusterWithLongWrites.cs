using System;
using System.Collections.Generic;

using SkbKontur.Cassandra.Primitives.Storages.Primitives;
using SkbKontur.Cassandra.ThriftClient.Clusters;
using SkbKontur.Cassandra.ThriftClient.Connections;
using SkbKontur.Cassandra.ThriftClient.Core.Pools;

namespace CassandraPrimitives.Tests.FunctionalTests.LongWritesConnection
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

        private readonly ICassandraCluster cluster;
        private readonly TimeSpan columnFamilyConnectionTimeout;
    }
}