using System.Collections.Generic;
using System.Linq;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Implementations.ZookeeperSettings;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Implementations.ZooKeeper;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.Agents;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.MainDriver
{
    public class ZookeeperMainDriver
    {
        public ZookeeperMainDriver(ITeamCityLogger teamCityLogger, List<RemoteAgentInfo> agents, string taskWrapperRelativePath, bool noDeploy)
        {
            this.teamCityLogger = teamCityLogger;
            this.agents = agents;
            this.taskWrapperRelativePath = taskWrapperRelativePath;
            this.noDeploy = noDeploy;
        }

        public ZookeeperClusterStarter StartZookeeperCluster()
        {
            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Initialising zookeeper...");

            var remoteZookeeperNodeStartInfos = GetZookeeperNodeInfos();

            var nodeAddresses = agents
                .Zip(remoteZookeeperNodeStartInfos, (agent, info) => string.Format("{0}:{1}", agent.IpAddress, info.Settings.ClientPort));
            var connectionString = string.Join(",", nodeAddresses);

            ClusterSettings = new ZookeeperClusterSettings(connectionString);

            return new ZookeeperClusterStarter(ClusterSettings, remoteZookeeperNodeStartInfos, teamCityLogger, noDeploy);
        }

        public ZookeeperClusterSettings ClusterSettings { get; private set; }

        private List<ZookeeperRemoteNodeStartInfo> GetZookeeperNodeInfos()
        {
            var addresses = agents.Select(agent => agent.IpAddress.ToString()).ToArray();
            return agents
                .Select((agent, i) =>
                        new ZookeeperRemoteNodeStartInfo(
                            agent.Credentials,
                            new ZookeeperNodeSettings(serverAddresses : addresses, id : i + 1),
                            agent.WorkDirectory,
                            taskWrapperRelativePath))
                .ToList();
        }

        private readonly ITeamCityLogger teamCityLogger;
        private readonly List<RemoteAgentInfo> agents;
        private readonly bool noDeploy;
        private readonly string taskWrapperRelativePath;
    }
}