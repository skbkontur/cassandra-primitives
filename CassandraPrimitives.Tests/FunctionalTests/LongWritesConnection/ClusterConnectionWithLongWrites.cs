using System;
using System.Collections.Generic;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Connections;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.LongWritesConnection
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

        public void WaitUntilSchemeAgreementIsReached(TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        private readonly IClusterConnection clusterConnection;
    }
}