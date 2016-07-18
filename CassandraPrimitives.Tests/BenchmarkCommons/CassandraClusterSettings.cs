using System;
using System.Net;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons
{
    public class CassandraClusterSettings : ICassandraClusterSettings
    {
        public CassandraClusterSettings(string clusterName, IPEndPoint[] endpoints, IPEndPoint endpointForFierceCommands)
        {
            ClusterName = clusterName;
            ReadConsistencyLevel = ConsistencyLevel.QUORUM;
            WriteConsistencyLevel = ConsistencyLevel.QUORUM;
            Endpoints = endpoints;
            EndpointForFierceCommands = endpointForFierceCommands;
            AllowNullTimestamp = false;
            Attempts = 5;
            Timeout = 6000;
            FierceTimeout = 10000;
            ConnectionIdleTimeout = TimeSpan.FromMinutes(1);
        }

        public string ClusterName { get; set; }
        public ConsistencyLevel ReadConsistencyLevel { get; private set; }
        public ConsistencyLevel WriteConsistencyLevel { get; private set; }
        public IPEndPoint[] Endpoints { get; set; }
        public IPEndPoint EndpointForFierceCommands { get; set; }
        public bool AllowNullTimestamp { get; set; }
        public int Attempts { get; set; }
        public int Timeout { get; set; }
        public int FierceTimeout { get; set; }
        public TimeSpan? ConnectionIdleTimeout { get; set; }
    }
}