using System.Collections.Generic;
using System.Linq;
using System.Net;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.CassandraInitialisation;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Implementations.Cassandra;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.Agents;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.MainDriver
{
    public class CassandraMainDriver
    {
        public CassandraMainDriver(ITeamCityLogger teamCityLogger, List<RemoteAgentInfo> agents, string taskWrapperRelativePath)
        {
            this.teamCityLogger = teamCityLogger;
            this.taskWrapperRelativePath = taskWrapperRelativePath;
            this.agents = agents;
        }

        public CassandraClusterSettings ClusterSettings { get; private set; }

        public CassandraClusterStarter StartCassandraCluster()
        {
            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Initialising cassandra...");

            var clusterName = "test_cluster";
            var remoteCassandraNodeStartInfos = GetCassandraNodeInfos(clusterName);
            var endpoints = remoteCassandraNodeStartInfos
                .Select(c => new IPEndPoint(IPAddress.Parse(c.Settings.ListenAddress), c.Settings.RpcPort))
                .ToArray();
            ClusterSettings = new CassandraClusterSettings(clusterName, endpoints, endpoints.First());

            return new CassandraClusterStarter(ClusterSettings, remoteCassandraNodeStartInfos);
        }

        private List<CassandraRemoteNodeStartInfo> GetCassandraNodeInfos(string clusterName)
        {
            var seedAddresses = agents.Select(agent => agent.IpAddress.ToString()).ToArray();
            return agents
                .Select(agent =>
                        new CassandraRemoteNodeStartInfo(
                            agent.Credentials,
                            new CassandraNodeSettings
                                (
                                name : clusterName,
                                listenAddress : agent.IpAddress.ToString(),
                                seedAddresses : seedAddresses,
                                rpcPort : 59360
                                ),
                            agent.WorkDirectory,
                            taskWrapperRelativePath))
                .ToList();
        }

        private readonly ITeamCityLogger teamCityLogger;
        private readonly List<RemoteAgentInfo> agents;
        private readonly string taskWrapperRelativePath;
    }
}