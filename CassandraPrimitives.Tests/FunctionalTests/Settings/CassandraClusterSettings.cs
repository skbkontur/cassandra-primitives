using System;
using System.Net;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.ClusterDeployment;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Settings
{
    public class CassandraClusterSettings : ICassandraClusterSettings
    {
        public string ClusterName { get; set; }
        public ConsistencyLevel ReadConsistencyLevel { get { return ConsistencyLevel.QUORUM; } }
        public ConsistencyLevel WriteConsistencyLevel { get { return ConsistencyLevel.QUORUM; } }
        public IPEndPoint[] Endpoints { get; set; }
        public IPEndPoint EndpointForFierceCommands { get; set; }
        public bool AllowNullTimestamp { get { return false; } }
        public int Attempts { get; set; }
        public int Timeout { get; set; }
        public int FierceTimeout { get { return (int)TimeSpan.FromSeconds(10.0).TotalMilliseconds; } }
        public TimeSpan? ConnectionIdleTimeout { get { return TimeSpan.FromMinutes(1.0); } }
        public bool EnableMetrics { get { return false; } }

        public static ICassandraClusterSettings ForNode(CassandraNode node, int attempts = 5, TimeSpan? timeout = null)
        {
            return new CassandraClusterSettings
                {
                    ClusterName = node.ClusterName,
                    Attempts = attempts,
                    Timeout = (int)(timeout ?? TimeSpan.FromSeconds(6)).TotalMilliseconds,
                    Endpoints = new[] {new IPEndPoint(IPAddress.Loopback, node.RpcPort)},
                    EndpointForFierceCommands = new IPEndPoint(IPAddress.Loopback, node.RpcPort),
                };
        }
    }
}