using System;
using System.Net;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Settings
{
    public class CassandraClusterSettings : ICassandraClusterSettings
    {
        public string ClusterName { get { return "CatalogueCluster"; } }
        public ConsistencyLevel ReadConsistencyLevel { get { return ConsistencyLevel.QUORUM; } }
        public ConsistencyLevel WriteConsistencyLevel { get { return ConsistencyLevel.QUORUM; } }
        public IPEndPoint[] Endpoints { get { return new[] {new IPEndPoint(new IPAddress(new byte[] {127, 0, 0, 1}), 9160)}; } }
        public IPEndPoint EndpointForFierceCommands { get { return  new IPEndPoint(new IPAddress(new byte[] { 127, 0, 0, 1 }), 9160); } }
        public bool AllowNullTimestamp { get { return false; } }
        public int Attempts { get { return 10; } }
        public int Timeout { get { return 6000; } }
        public int FierceTimeout { get { return 60000; } }
        public TimeSpan? ConnectionIdleTimeout { get { return TimeSpan.FromMinutes(5); } }
    }
}