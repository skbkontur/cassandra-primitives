using System;
using System.Net;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark
{
    public class CassandraClusterSettings : ICassandraClusterSettings
    {
        public string ClusterName { get; set; }
        public ConsistencyLevel ReadConsistencyLevel { get; set; }
        public ConsistencyLevel WriteConsistencyLevel { get; set; }
        public IPEndPoint[] Endpoints { get; set; }
        public IPEndPoint EndpointForFierceCommands { get; set; }
        public bool AllowNullTimestamp { get; set; }
        public int Attempts { get; set; }
        public int Timeout { get; set; }
        public int FierceTimeout { get; set; }
        public TimeSpan? ConnectionIdleTimeout { get; set; }

        public static CassandraClusterSettings FromICassandraClusterSettings(ICassandraClusterSettings clusterSettings)
        {
            return new CassandraClusterSettings
                {
                    ClusterName = clusterSettings.ClusterName,
                    ReadConsistencyLevel = clusterSettings.ReadConsistencyLevel,
                    WriteConsistencyLevel = clusterSettings.WriteConsistencyLevel,
                    Endpoints = clusterSettings.Endpoints,
                    EndpointForFierceCommands = clusterSettings.EndpointForFierceCommands,
                    AllowNullTimestamp = clusterSettings.AllowNullTimestamp,
                    Attempts = clusterSettings.Attempts,
                    Timeout = clusterSettings.Timeout,
                    FierceTimeout = clusterSettings.FierceTimeout,
                    ConnectionIdleTimeout = clusterSettings.ConnectionIdleTimeout,
                };
        }
    }
}