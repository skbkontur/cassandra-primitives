using System;
using System.Collections.Generic;

using SkbKontur.Cassandra.ThriftClient.Abstractions;
using SkbKontur.Cassandra.ThriftClient.Connections;

namespace CassandraPrimitives.Tests.FunctionalTests.LongWritesConnection
{
    public class ClusterConnectionWithLongWrites : IClusterConnection
    {
        public ClusterConnectionWithLongWrites(IClusterConnection clusterConnection)
        {
            this.clusterConnection = clusterConnection;
        }

        public IList<Keyspace> RetrieveKeyspaces()
        {
            return clusterConnection.RetrieveKeyspaces();
        }

        public void AddKeyspace(Keyspace keyspace)
        {
            clusterConnection.AddKeyspace(keyspace);
        }

        public void UpdateKeyspace(Keyspace keyspace)
        {
            clusterConnection.UpdateKeyspace(keyspace);
        }

        public void RemoveKeyspace(string keyspace)
        {
            clusterConnection.RemoveKeyspace(keyspace);
        }

        public string DescribeVersion()
        {
            return clusterConnection.DescribeVersion();
        }

        public void WaitUntilSchemaAgreementIsReached(TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        private readonly IClusterConnection clusterConnection;
    }
}