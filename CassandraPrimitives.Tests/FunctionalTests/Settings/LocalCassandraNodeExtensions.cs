using System;
using System.Linq;
using System.Net;

using JetBrains.Annotations;

using SkbKontur.Cassandra.ThriftClient.Clusters;

namespace CassandraPrimitives.Tests.FunctionalTests.Settings
{
    public static class LocalCassandraSettingsFactory
    {
        public static ICassandraClusterSettings CreateSettings(int attempts = 5, TimeSpan? timeout = null)
        {
            return new SingleNodeCassandraClusterSettings(new IPEndPoint(GetIpV4Address("127.0.0.1"), 9160))
                {
                    ClusterName = "TestCluster",
                    Attempts = attempts,
                    Timeout = (int)(timeout ?? TimeSpan.FromSeconds(6)).TotalMilliseconds,
                };
        }

        private static IPAddress GetIpV4Address([NotNull] string hostNameOrIpAddress)
        {
            if (IPAddress.TryParse(hostNameOrIpAddress, out var res))
                return res;

            return Dns.GetHostEntry(hostNameOrIpAddress).AddressList.First(address => !address.ToString().Contains(':'));
        }
    }
}