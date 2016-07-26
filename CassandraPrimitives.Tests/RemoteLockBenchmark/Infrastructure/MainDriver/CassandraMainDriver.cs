using System.Collections.Generic;
using System.Linq;
using System.Net;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.CassandraInitialisation;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Implementations.Cassandra;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.Agents;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.MainDriver
{
    public class CassandraMainDriver
    {
        public CassandraMainDriver(ITeamCityLogger teamCityLogger, List<RemoteAgentInfo> agents, bool noDeploy)
        {
            this.teamCityLogger = teamCityLogger;
            this.noDeploy = noDeploy;
            this.agents = agents;
        }

        public CassandraClusterSettings ClusterSettings { get; private set; }

        public CassandraClusterStarter StartCassandraCluster()
        {
            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Initialising cassandra...");

            var clusterName = "test_cluster";
            var remoteCassandraNodeStartInfos = GetCassandraNodeInfos(agents, clusterName);
            var endpoints = remoteCassandraNodeStartInfos
                .Select(c => new IPEndPoint(IPAddress.Parse(c.Settings.ListenAddress), c.Settings.RpcPort))
                .ToArray();
            ClusterSettings = new CassandraClusterSettings(clusterName, endpoints, endpoints.First());

            return new CassandraClusterStarter(ClusterSettings, remoteCassandraNodeStartInfos, noDeploy);
        }

        private static List<CassandraRemoteNodeStartInfo> GetCassandraNodeInfos(List<RemoteAgentInfo> agents, string clusterName)
        {
            var seedAddresses = agents.Select(agent => agent.IpAddress.ToString()).ToArray();
            return agents
                .Select(agent =>
                        new CassandraRemoteNodeStartInfo(
                            agent.Credentials,
                            new CassandraNodeSettings
                                {
                                    ClusterName = clusterName,
                                    ListenAddress = Dns.GetHostAddresses(agent.Name).First().ToString(),
                                    SeedAddresses = seedAddresses,
                                    RpcPort = 59360
                                },
                            agent.WorkDirectory))
                .ToList();
        }

        private readonly ITeamCityLogger teamCityLogger;
        private readonly bool noDeploy;
        private readonly List<RemoteAgentInfo> agents;
    }
}