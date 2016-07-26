using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.CassandraInitialisation;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.Logging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.RemoteTaskRunning;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Agents;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Cassandra;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.ExternalLogging.HttpLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.ExternalLogging.TestProgressProcessors;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Processes;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.ZooKeeper;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.TestConfigurations;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.ZookeeperSettings;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark
{
    public class MainDriver
    {
        public MainDriver(ITeamCityLogger teamCityLogger, TestConfiguration configuration, IAgentProvider agentProvider, bool noDeploy = false)
        {
            Log4NetConfiguration.InitializeOnce();
            this.teamCityLogger = teamCityLogger;
            this.configuration = configuration;
            this.noDeploy = noDeploy;
            this.agentProvider = agentProvider;
        }

        public List<RemoteAgentInfo> PrepareAgentsFor(string target, int amount)
        {
            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Choosing agents for {0}", target);
            var agents = agentProvider.AcquireAgents(amount);
            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Agents for {0}: {1}", target, string.Join(", ", agents.Select(agent => agent.Name)));
            foreach (var agent in agents)
                DeployWrapper(agent.WorkDirectory);
            return agents;
        }

        public void PrepareAgents()
        {
            if (configuration.RemoteLockImplementation == RemoteLockImplementations.Cassandra)
                cassandraAgents = PrepareAgentsFor("Cassandra", 3);
            if (configuration.RemoteLockImplementation == RemoteLockImplementations.Zookeeper)
                zookeeperAgents = PrepareAgentsFor("Zookeeper", 3);
            testAgents = PrepareAgentsFor("Tests", configuration.AmountOfProcesses);
        }

        public CassandraClusterStarter CreateCassandraClusterStarter()
        {
            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Initialising cassandra...");

            var clusterName = "test_cluster";
            var remoteCassandraNodeStartInfos = GetCassandraNodeInfos(cassandraAgents, clusterName);
            var endpoints = remoteCassandraNodeStartInfos
                .Select(c => new IPEndPoint(IPAddress.Parse(c.Settings.ListenAddress), c.Settings.RpcPort))
                .ToArray();
            cassandraClusterSettings = new CassandraClusterSettings(clusterName, endpoints, endpoints.First());

            return new CassandraClusterStarter(cassandraClusterSettings, remoteCassandraNodeStartInfos, noDeploy);
        }

        private ZookeeperClusterStarter CreateZooKeeperClusterStarter()
        {
            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Initialising zookeeper...");

            var remoteZookeeperNodeStartInfos = GetZookeeperNodeInfos(zookeeperAgents);

            var nodeAddresses = zookeeperAgents
                .Select(agent => Dns.GetHostAddresses(agent.Name).First())
                .Zip(remoteZookeeperNodeStartInfos, (ip, info) => string.Format("{0}:{1}", ip, info.Settings.ClientPort));
            var connectionString = string.Join(",", nodeAddresses);

            zookeeperClusterSettings = new ZookeeperClusterSettings(connectionString);

            return new ZookeeperClusterStarter(zookeeperClusterSettings, remoteZookeeperNodeStartInfos, teamCityLogger, noDeploy);
        }

        private void DeployWrapper(RemoteDirectory workDir)
        {
            if (noDeploy)
                return;
            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Deploying wrapper to {0}", workDir.AsRemote);
            var source = new DirectoryInfo(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "..", "..", "..", "..", "Assemblies", "TaskWrapper"));
            var remoteDir = Path.Combine(workDir.AsRemote, "TaskWrapper");
            source.CopyTo(new DirectoryInfo(remoteDir), true);
        }

        public static List<CassandraRemoteNodeStartInfo> GetCassandraNodeInfos(List<RemoteAgentInfo> agents, string clusterName)
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

        public static List<ZookeeperRemoteNodeStartInfo> GetZookeeperNodeInfos(List<RemoteAgentInfo> agents)
        {
            var addresses = agents.Select(agent => agent.IpAddress.ToString()).ToArray();
            return agents
                .Select((agent, i) =>
                        new ZookeeperRemoteNodeStartInfo(
                            agent.Credentials,
                            new ZookeeperNodeSettings(serverAddresses : addresses, id : i + 1),
                            agent.WorkDirectory))
                .ToList();
        }

        public void Run()
        {
            teamCityLogger.BeginMessageBlock("Results");

            PrepareAgents();

            //using (CreateCassandraClusterStarter())
            using (CreateZooKeeperClusterStarter())
            {
                try
                {
                    using (new HttpTestDataProvider(cassandraClusterSettings, zookeeperClusterSettings, configuration))
                    using (var testProcessor = new TimelineTestProgressProcessor(configuration, teamCityLogger))
                    using (new HttpExternalLogProcessor(configuration, teamCityLogger, testAgents, testProcessor))
                    using (var processLauncher = new RemoteProcessLauncher(teamCityLogger, testAgents, noDeploy))
                    {
                        processLauncher.StartProcesses(configuration);

                        processLauncher.WaitForProcessesToFinish();

                        teamCityLogger.EndMessageBlock();

                        teamCityLogger.SetBuildStatus(TeamCityBuildStatus.Success, "Done");
                    }
                }
                catch (Exception e)
                {
                    teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Failure, "Exception occured while working with child processes:\n{0}", e);
                    teamCityLogger.EndMessageBlock();
                    teamCityLogger.SetBuildStatus("Fail", "Fail because of unexpected exceptions");
                }
            }
        }

        private List<RemoteAgentInfo> cassandraAgents, zookeeperAgents, testAgents;
        private readonly ITeamCityLogger teamCityLogger;
        private readonly TestConfiguration configuration;
        private readonly bool noDeploy;
        private CassandraClusterSettings cassandraClusterSettings;
        private ZookeeperClusterSettings zookeeperClusterSettings;
        private readonly IAgentProvider agentProvider;
    }
}