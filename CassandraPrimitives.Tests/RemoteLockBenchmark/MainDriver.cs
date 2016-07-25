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
        public MainDriver(ITeamCityLogger teamCityLogger, TestConfiguration configuration, bool noDeploy = false)
        {
            Log4NetConfiguration.InitializeOnce();
            this.teamCityLogger = teamCityLogger;
            this.configuration = configuration;
            this.noDeploy = noDeploy;
        }

        public void PrepareAgents()
        {
            var agentProvider = new AgentProviderAllAgents();
            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Choosing agents...");
            if (configuration.remoteLockImplementation == TestConfiguration.RemoteLockImplementation.Cassandra)
            {
                cassandraAgents = agentProvider.GetAgents(1);
                teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Cassandra agents: {0}", String.Join(", ", cassandraAgents.Select(agent => agent.Name)));
                foreach (var agent in cassandraAgents)
                    DeployWrapper(agent.WorkDirectory);
            }
            if (configuration.remoteLockImplementation == TestConfiguration.RemoteLockImplementation.Zookeeper)
            {
                zookeeperAgents = agentProvider.GetAgents(1);
                teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Zookeeper agents: {0}", String.Join(", ", zookeeperAgents.Select(agent => agent.Name)));
                foreach (var agent in zookeeperAgents)
                    DeployWrapper(agent.WorkDirectory);
            }
            testAgents = agentProvider.GetAgents(configuration.amountOfProcesses);
            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Test agents: {0}", String.Join(", ", testAgents.Select(agent => agent.Name)));
            foreach (var agent in testAgents)
                DeployWrapper(agent.WorkDirectory);
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
            var address = Dns.GetHostAddresses(zookeeperAgents.Single().Name).First().ToString();
            var port = remoteZookeeperNodeStartInfos.Single().Settings.ClientPort;
            var connectionString = String.Format("{0}:{1}", address, port);
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
            var seedAddresses = agents.Select(agent => Dns.GetHostAddresses(agent.Name).First().ToString()).ToArray();
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
            return agents
                .Select(agent =>
                        new ZookeeperRemoteNodeStartInfo(
                            agent.Credentials,
                            new ZookeeperNodeSettings(),
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
    }
}