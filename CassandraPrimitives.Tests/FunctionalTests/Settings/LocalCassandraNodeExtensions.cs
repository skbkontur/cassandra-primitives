using System;
using System.Net;

using SkbKontur.Cassandra.Local;

using SKBKontur.Cassandra.CassandraClient.Clusters;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Settings
{
    public static class LocalCassandraNodeExtensions
    {
        public static ICassandraClusterSettings CreateSettings(this LocalCassandraNode node, int attempts = 5, TimeSpan? timeout = null)
        {
            return new SingleNodeCassandraClusterSettings(new IPEndPoint(IPAddress.Parse(node.RpcAddress), node.RpcPort))
                {
                    ClusterName = node.ClusterName,
                    Attempts = attempts,
                    Timeout = (int)(timeout ?? TimeSpan.FromSeconds(6)).TotalMilliseconds,
                };
        }
    }
}