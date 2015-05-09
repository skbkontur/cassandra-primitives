using System;
using System.Linq;
using System.Net;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Catalogue.Core.Configuration.Settings;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.TimeService.Settings
{
    public class CassandraClusterSettings : ICassandraClusterSettings
    {
        public CassandraClusterSettings(IApplicationSettings applicationSettings)
        {
            ClusterName = applicationSettings.GetString("ClusterName");
            ReadConsistencyLevel = applicationSettings.GetEnum<ConsistencyLevel>("ReadConsistencyLevel");
            WriteConsistencyLevel = applicationSettings.GetEnum<ConsistencyLevel>("WriteConsistencyLevel");
            Endpoints = applicationSettings.GetStringArray("Endpoints").Select(ParseEndPoint).ToArray();
            EndpointForFierceCommands = ParseEndPoint(applicationSettings.GetString("EndpointForFierceCommands"));
            Attempts = applicationSettings.GetInt("Attempts");
            Timeout = applicationSettings.GetInt("Timeout");
            FierceTimeout = applicationSettings.GetInt("FierceTimeout");
            TimeSpan checkConnectionsInterval;
            ConnectionIdleTimeout = applicationSettings.TryGetTimeSpan("ConnectionIdleTimeout", out checkConnectionsInterval) ? checkConnectionsInterval : (TimeSpan?)null;
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