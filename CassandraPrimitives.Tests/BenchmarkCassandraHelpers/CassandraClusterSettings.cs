using System;
using System.Linq;
using System.Net;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;

namespace BenchmarkCassandraHelpers
{
    public class CassandraClusterSettings : ICassandraClusterSettings
    {
        public CassandraClusterSettings()
        {
            ClusterName = "test_cluster";
            ReadConsistencyLevel = ConsistencyLevel.QUORUM;
            WriteConsistencyLevel = ConsistencyLevel.QUORUM;
            Endpoints = new[] { ParseEndPoint("alco-cs-01:9360"), ParseEndPoint("alco-cs-02:9360"), ParseEndPoint("alco-cs-03:9360"),};
            EndpointForFierceCommands = ParseEndPoint("alco-cs-01:9360");
//            Endpoints = new[] { ParseEndPoint("127.0.0.1:9360") };
//            EndpointForFierceCommands = ParseEndPoint("127.0.0.1:9360");
            Attempts = 20;
            Timeout = 6000;
            FierceTimeout = 6000;
            ConnectionIdleTimeout = TimeSpan.FromMinutes(10);
            AllowNullTimestamp = false;
        }

        public string ClusterName { get; private set; }
        public ConsistencyLevel ReadConsistencyLevel { get; private set; }
        public ConsistencyLevel WriteConsistencyLevel { get; private set; }
        public IPEndPoint[] Endpoints { get; private set; }
        public IPEndPoint EndpointForFierceCommands { get; private set; }
        public bool AllowNullTimestamp { get; private set; }
        public int Attempts { get; private set; }
        public int Timeout { get; private set; }
        public int FierceTimeout { get; private set; }
        public TimeSpan? ConnectionIdleTimeout { get; private set; }

        public static IPEndPoint ParseEndPoint(string s)
        {
            var spitted = s.Split(':');
            return new IPEndPoint(GetIpV4Address(spitted[0]), int.Parse(spitted[1]));
        }

        private static IPAddress GetIpV4Address(string hostNameOrIpAddress = null)
        {
            IPAddress res;
            if (!string.IsNullOrEmpty(hostNameOrIpAddress) && IPAddress.TryParse(hostNameOrIpAddress, out res))
                return res;
            var addresses = Dns.GetHostEntry(hostNameOrIpAddress ?? Dns.GetHostName());
            return addresses.AddressList.First(address => !address.ToString().Contains(':'));
        }
    }
}