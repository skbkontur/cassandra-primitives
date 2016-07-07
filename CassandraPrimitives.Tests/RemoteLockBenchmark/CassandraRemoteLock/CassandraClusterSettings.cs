using System;
using System.Net;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.CassandraRemoteLock
{
    public class CassandraClusterSettings : ICassandraClusterSettings
    {
        public string ClusterName { get { return "test_cluster"; } }
        public ConsistencyLevel ReadConsistencyLevel { get { return ConsistencyLevel.QUORUM; } }
        public ConsistencyLevel WriteConsistencyLevel { get { return ConsistencyLevel.QUORUM; } }
        public IPEndPoint[] Endpoints { get { return new[] {new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9360)}; } }
        public IPEndPoint EndpointForFierceCommands { get { return new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9360); } }
        public bool AllowNullTimestamp { get { return false; } }
        public int Attempts { get { return 5; } }
        public int Timeout { get { return 6000; } }
        public int FierceTimeout { get { return 10000; } }
        public TimeSpan? ConnectionIdleTimeout { get { return TimeSpan.FromMinutes(1); } }
    }
}