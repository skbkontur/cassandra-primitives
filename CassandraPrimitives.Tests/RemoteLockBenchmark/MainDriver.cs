﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;

using Newtonsoft.Json;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.CassandraInitialisation;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.RemoteTaskRunning;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Agents;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.CassandraRemoteLock;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.ExternalLogging.HttpLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Logging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Processes;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.TestConfigurations;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark
{
    public class HttpTestDataProvider : IDisposable
    {
        public HttpTestDataProvider(CassandraClusterSettings cassandraClusterSettings)
        {
            server = new HttpServer();
            server.AddMethod("get_cassandra_options", async context =>
                {
                    try
                    {
                        using (var stream = new StreamWriter(context.Response.OutputStream))
                        {
                            JsonSerializerSettings settings = new JsonSerializerSettings();
                            settings.Converters.Add(new IpAddressConverter());
                            settings.Converters.Add(new IpEndPointConverter());
                            settings.Formatting = Formatting.Indented;
                            await stream.WriteAsync(JsonConvert.SerializeObject(cassandraClusterSettings, settings));
                        }
                        context.Response.OutputStream.Close();
                    }
                    finally
                    {
                        context.Response.Close();
                    }
                });
        }

        public void Dispose()
        {
            server.Dispose();
        }

        private readonly HttpServer server;
    }

    public class MainDriver
    {
        public MainDriver(ITeamCityLogger teamCityLogger, TestConfiguration configuration)
        {
            Log4NetConfiguration.InitializeOnce();
            this.teamCityLogger = teamCityLogger;
            this.configuration = configuration;
        }

        public void PrepareAgents()
        {
            var agentProvider = new AgentProviderWithSingleAgent();
            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Choosing agents...");
            cassandraAgents = agentProvider.GetAgents(1);
            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Cassandra agents: {0}", String.Join(", ", cassandraAgents.Select(agent => agent.Name)));
            testAgents = agentProvider.GetAgents(configuration.amountOfProcesses);
            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Test agents: {0}", String.Join(", ", testAgents.Select(agent => agent.Name)));
            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Deploying wrapper...");
            foreach (var agent in cassandraAgents.Concat(testAgents))
                DeployWrapper(agent.Credentials, agent.WorkDirectory);
        }

        public void PrepareCassandraSettings()
        {
            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Initialising cassandra...");

            var clusterName = "test_cluster";
            remoteCassandraNodeStartInfos = GetCassandraNodeInfos(cassandraAgents, clusterName);
            var endpoints = remoteCassandraNodeStartInfos
                .Select(c => new IPEndPoint(IPAddress.Parse(c.Settings.ListenAddress), c.Settings.RpcPort))
                .ToArray();
            cassandraClusterSettings = new CassandraClusterSettings(clusterName, endpoints, endpoints.First());
        }

        private static void DeployWrapper(RemoteMachineCredentials credentials, RemoteDirectory workDir)
        {
            using (new ImpersonateUser(credentials))
            {
                var source = new DirectoryInfo(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "JobObjects"));
                var remoteDir = Path.Combine(workDir.AsRemote, "JobObjects");
                source.CopyTo(new DirectoryInfo(remoteDir), true);
            }
        }

        public static List<CassandraRemoteNodeStartInfo> GetCassandraNodeInfos(List<RemoteAgentInfo> agents, string clusterName)
        {
            var seedAddresses = agents.Select(agent => Dns.GetHostAddresses(agent.Name).First().ToString()).ToArray();
            //var seedAddresses = agents.Select(agent => "127.0.0.1").ToArray();
            return agents
                .Select(agent =>
                        new CassandraRemoteNodeStartInfo(
                            agent.Credentials,
                            new CassandraNodeSettings
                                {
                                    ClusterName = clusterName,
                                    ListenAddress = Dns.GetHostAddresses(agent.Name).First().ToString(),
                                    //ListenAddress = "127.0.0.1",
                                    SeedAddresses = seedAddresses,
                                    RpcPort = 59360
                                },
                            agent.WorkDirectory))
                .ToList();
        }

        public void Run()
        {
            teamCityLogger.BeginMessageBlock("Results");

            PrepareAgents();
            PrepareCassandraSettings();

            using (new CassandraClusterStarter(cassandraClusterSettings, remoteCassandraNodeStartInfos))
            {
                try
                {
                    using (new HttpTestDataProvider(cassandraClusterSettings))
                    using (new HttpExternalLogProcessor(configuration, teamCityLogger))
                    using (var processLauncher = new RemoteProcessLauncher(teamCityLogger, testAgents))
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

        private List<RemoteAgentInfo> cassandraAgents, testAgents;
        private readonly ITeamCityLogger teamCityLogger;
        private readonly TestConfiguration configuration;
        private CassandraClusterSettings cassandraClusterSettings;
        private List<CassandraRemoteNodeStartInfo> remoteCassandraNodeStartInfos;
    }
}