using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;

using Cassandra;

using Metrics;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.CassandraInitialisation;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.JmxInitialisation;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.RemoteTaskRunning;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Implementations.Cassandra;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Implementations.ZooKeeper.ZookeeperSettings;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.Agents.Providers;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.ChildProcessDriver;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.MainDriver;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.Registry;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.TestConfigurations;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.TestOptions;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.TestProgressProcessors;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.SchemeActualizer;
using SKBKontur.Catalogue.TeamCity;

using ConsistencyLevel = SKBKontur.Cassandra.CassandraClient.Abstractions.ConsistencyLevel;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.BenchmarkConfiguration
{
    internal class SingleRunBenchmarkConfigurator : IBenchmarkConfigurator
    {
        private SingleRunBenchmarkConfigurator(TestConfiguration testConfiguration, ITestOptions testOptions, Func<IScenariosRegistry> staticRegistryCreatorMethod, MetricsContext metricsContext)
        {
            this.testConfiguration = testConfiguration;
            this.testOptions = testOptions;
            registryCreator = staticRegistryCreatorMethod;
            this.metricsContext = metricsContext;
            innerAdditionalJmxHosts = new List<Tuple<string, int>>();
            deploySteps = new List<DeployStep>();
            teamCityLogger = new FakeTeamCityLogger();
            toDispose = new List<IDisposable>();
            optionsSet = new Dictionary<string, object>();
            dynamicOptionsSet = new Dictionary<string, Func<object>>();
            onAllProcessesStarted = () => { };
        }

        internal static IBenchmarkConfigurator CreateNew(TestConfiguration configuration, ITestOptions testOptions, Func<IScenariosRegistry> staticRegistryCreatorMethod, MetricsContext metricsContext)
        {
            return new SingleRunBenchmarkConfigurator(configuration, testOptions, staticRegistryCreatorMethod, metricsContext);
        }

        public IReadyToStartBenchmarkConfigurator WithTeamCityLogger(ITeamCityLogger teamCityLogger)
        {
            this.teamCityLogger = teamCityLogger;
            return this;
        }

        public IReadyToStartBenchmarkConfigurator WithOption(string name, object value)
        {
            optionsSet[name] = value;
            return this;
        }

        public IReadyToStartBenchmarkConfigurator WithDynamicOption(string name, Func<object> valueProvider)
        {
            dynamicOptionsSet[name] = valueProvider;
            return this;
        }

        public IReadyToStartBenchmarkConfigurator WithAllProcessStartedHandler(Action onAllProcessesStarted)
        {
            this.onAllProcessesStarted = onAllProcessesStarted;
            return this;
        }

        public IReadyToStartBenchmarkConfigurator WithDefaultTeamCityLogger()
        {
            teamCityLogger = new TeamCityLogger(Console.Out);
            return this;
        }

        public IReadyToStartBenchmarkConfigurator WithTeamCityLogger()
        {
            teamCityLogger = new TeamCityLogger(Console.Out);
            return this;
        }

        public IReadyToStartBenchmarkConfigurator WithAgentProvider(IAgentProvider agentProvider)
        {
            this.agentProvider = agentProvider;
            return this;
        }

        public IReadyToStartBenchmarkConfigurator WithAgentProviderFromTeamCity()
        {
            throw new NotSupportedException();
        }

        public IReadyToStartBenchmarkConfigurator WithAgentProviderFromTeamCity(IEnvironmentVariableProvider variableProvider)
        {
            agentProvider = new AgentProviderFromTeamCity(variableProvider);
            return this;
        }

        public IReadyToStartBenchmarkConfigurator WithJmxTrans(string graphitePrefix)
        {
            return WithJmxTrans(graphitePrefix, new Tuple<string, int>[0]);
        }

        public IReadyToStartBenchmarkConfigurator WithJmxTrans(string graphitePrefix, Tuple<string, int>[] additionalJmxHosts)
        {
            deploySteps.Add(new DeployStep("JmxTrans deploy", () =>
                {
                    var wrapperDeployer = new WrapperDeployer(teamCityLogger);
                    wrapperDeployer.DeployWrapper(new RemoteDirectory(AppDomain.CurrentDomain.BaseDirectory, "", ""));
                    var initialiser = new JmxTransInitialiser(TasksSettings.TasksGroup);
                    var deployDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "JmxTrans");
                    var agentNames = agentProvider.GetAllAgentNames();
                    var settingsList = agentNames
                        .Select(name => new JmxSettings(name, name.Split('.').First(), graphitePrefix, 7399))
                        .Concat(innerAdditionalJmxHosts.Select(host => new JmxSettings(host.Item1, host.Item1.Split('.').First(), graphitePrefix, host.Item2)))
                        .Concat(additionalJmxHosts.Select(host => new JmxSettings(host.Item1, host.Item1.Split('.').First(), graphitePrefix, host.Item2)))
                        .ToList();
                    initialiser.DeployJmxTrans(deployDirectory, settingsList);
                    toDispose.Add(initialiser.RunJmxTrans(deployDirectory, Path.Combine(AppDomain.CurrentDomain.BaseDirectory, wrapperDeployer.GetWrapperRelativePath())));
                }, DeployPriorities.JmxTrans));
            return this;
        }

        public IReadyToStartBenchmarkConfigurator WithSetUpAction(Action action)
        {
            deploySteps.Add(new DeployStep("SetUp action", action, DeployPriorities.SetUpAction));
            return this;
        }

        public IReadyToStartBenchmarkConfigurator WithTearDownAction(Action action)
        {
            deploySteps.Add(new DeployStep("TearDown action", action, DeployPriorities.TearDownAction));
            return this;
        }

        public IReadyToStartBenchmarkConfigurator WithCassandraCluster(ICassandraMetadataProvider cassandraMetadataProvider)
        {
            deploySteps.Add(new DeployStep("Cassandra deploy", () =>
                {
                    var cassandraAgents = agentProvider.AcquireAgents(testConfiguration.AmountOfClusterNodes);
                    var wrapperDeployer = new WrapperDeployer(teamCityLogger);
                    wrapperDeployer.DeployWrapperToAgents(cassandraAgents);
                    var cassandraDriver = new CassandraMainDriver(teamCityLogger, cassandraAgents, wrapperDeployer.GetWrapperRelativePath());
                    toDispose.Add(cassandraDriver.StartCassandraCluster(cassandraMetadataProvider));
                    CassandraSessionProvider.InitOnce(cassandraDriver.ClusterSettings.Endpoints.Select(ep => new IPEndPoint(ep.Address, 9343)).ToArray(), cassandraMetadataProvider.GetColumnFamilies().Single().KeyspaceName);
                    optionsSet["CassandraClusterSettings"] = cassandraDriver.ClusterSettings;
                }, DeployPriorities.Cluster));
            return this;
        }

        public IReadyToStartBenchmarkConfigurator WithExistingCassandraCluster(CassandraClusterSettings clusterSettings, ICassandraMetadataProvider cassandraMetadataProvider)
        {
            deploySteps.Add(new DeployStep("Configure cassandra cluster", () =>
                {
                    var cluster = Cluster
                        .Builder()
                        .AddContactPoints(clusterSettings.Endpoints.Select(ep => new IPEndPoint(ep.Address, 9042)).ToArray())
                        .WithQueryOptions(new QueryOptions().SetConsistencyLevel(global::Cassandra.ConsistencyLevel.Quorum))
                        .WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()))
                        .WithDefaultKeyspace(cassandraMetadataProvider.GetColumnFamilies().Single().ColumnFamilyName)
                        .Build();

                    var session = cluster.ConnectAndCreateDefaultKeyspaceIfNotExists(ReplicationStrategies.CreateSimpleStrategyReplicationProperty(3));

                    session.Execute(string.Format("CREATE TABLE IF NOT EXISTS \"{0}\" (", CassandraCqlBaseLockOperationsPerformer.MainTableName) +
                                        "lock_id varchar," +
                                        "threshold varchar," +
                                        "thread_id varchar," +
                                        "PRIMARY KEY ((lock_id), threshold, thread_id)" +
                                        ") WITH COMPACT STORAGE;", global::Cassandra.ConsistencyLevel.All);

                    session.Execute(string.Format("CREATE TABLE IF NOT EXISTS \"{0}\" (", CassandraCqlBaseLockOperationsPerformer.MetadataTableName) +
                                        "key varchar PRIMARY KEY," +
                                        "lock_row_id varchar," +
                                        "lock_count int," +
                                        "previous_threshold bigint," +
                                        "probable_owner_thread_id varchar," +
                                        "timestamp bigint" +
                                        ") WITH COMPACT STORAGE;", global::Cassandra.ConsistencyLevel.All);

                    /*using (var cassandraCluster = new CassandraCluster(clusterSettings))
                    {
                        var initializerSettings = new CassandraInitializerSettings(0, Math.Min(clusterSettings.Endpoints.Length, 3));
                        var cassandraSchemeActualizer = new CassandraSchemeActualizer(cassandraCluster, cassandraMetadataProvider, initializerSettings);
                        cassandraSchemeActualizer.AddNewColumnFamilies();
                    }*/
                    optionsSet["CassandraClusterSettings"] = clusterSettings;
                }, DeployPriorities.Cluster));
            return this;
        }

        public IReadyToStartBenchmarkConfigurator WithZookeeperCluster()
        {
            deploySteps.Add(new DeployStep("Zookeeper deploy", () =>
                {
                    var zookeeperAgents = agentProvider.AcquireAgents(testConfiguration.AmountOfClusterNodes);
                    var wrapperDeployer = new WrapperDeployer(teamCityLogger);
                    wrapperDeployer.DeployWrapperToAgents(zookeeperAgents);
                    var zookeeperDriver = new ZookeeperMainDriver(teamCityLogger, zookeeperAgents, wrapperDeployer.GetWrapperRelativePath());
                    toDispose.Add(zookeeperDriver.StartZookeeperCluster());
                    optionsSet["ZookeeperClusterSettings"] = zookeeperDriver.ClusterSettings;
                }, DeployPriorities.Cluster));
            return this;
        }

        public IReadyToStartBenchmarkConfigurator WithExistingZookeeperCluster(ZookeeperClusterSettings clusterSettings)
        {
            deploySteps.Add(new DeployStep("Configure Zookeeper cluster", () => { optionsSet["ZookeeperClusterSettings"] = clusterSettings; }, DeployPriorities.Cluster));
            return this;
        }

        public IReadyToStartBenchmarkConfigurator WithClusterFromConfiguration(ICassandraMetadataProvider cassandraMetadataProvider)
        {
            switch (testConfiguration.ClusterType)
            {
            case ClusterTypes.Cassandra:
                return WithCassandraCluster(cassandraMetadataProvider);
            case ClusterTypes.DeployedCassandra:
                innerAdditionalJmxHosts.AddRange(testConfiguration.ClusterEndpoints.Select(ep => Tuple.Create(ep, 7199)));
                var endpoints = testConfiguration
                    .ClusterEndpoints
                    .Select(name => Dns.GetHostAddresses(name).First(addr => addr.AddressFamily == AddressFamily.InterNetwork))
                    .Select(addr => new IPEndPoint(addr, testConfiguration.ClusterPort)).ToArray();
                return WithExistingCassandraCluster(new CassandraClusterSettings("name_not_defined", endpoints, endpoints.First()), cassandraMetadataProvider);
            case ClusterTypes.Zookeeper:
                return WithZookeeperCluster();
            case ClusterTypes.DeployedZookeeper:
                innerAdditionalJmxHosts.AddRange(testConfiguration.ClusterEndpoints.Select(ep => Tuple.Create(ep, 7199)));
                var endpointAddresses = testConfiguration
                    .ClusterEndpoints
                    .Select(name => Dns.GetHostAddresses(name).First(addr => addr.AddressFamily == AddressFamily.InterNetwork));
                var connectionString = string.Join(",", endpointAddresses.Select(addr => addr.ToString() + ":" + testConfiguration.ClusterPort));
                return WithExistingZookeeperCluster(new ZookeeperClusterSettings(connectionString));
            default:
                throw new Exception(string.Format("Type of cluster for {0} is unknown", testConfiguration.ClusterType));
            }
        }

        public void StartAndWaitForFinish()
        {
            var lastStep = "Preparation";
            try
            {
                deploySteps.Add(new DeployStep("TasksStopping", StopTasks, DeployPriorities.TasksStopping));
                deploySteps.Add(new DeployStep("MainProcess", MainProcess, DeployPriorities.Driver));
                foreach (var deployStep in deploySteps.OrderBy(s => s.Priority))
                {
                    lastStep = deployStep.Name;
                    teamCityLogger.BeginMessageBlock(deployStep.Name);
                    deployStep.Action.Invoke();
                    teamCityLogger.EndMessageBlock();
                }
            }
            catch (Exception e)
            {
                teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Failure, "Fail, exception occured in {0} step:\n{1}", lastStep, e);
            }
            finally
            {
                foreach (var disposable in toDispose)
                    disposable.Dispose();
            }
        }

        private ITestProgressProcessor GetTestProgressProcessor()
        {
            var options = new ProgressMessageProcessorCreationOptions(testConfiguration, teamCityLogger, metricsContext, testOptions);
            return registryCreator().CreateProcessor(testConfiguration.TestScenario, options);
        }

        private void StopTasks()
        {
            try
            {
                foreach (var agentName in agentProvider.GetAllAgentNames())
                {
                    teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Stopping tasks from group {0} on machine {1}", TasksSettings.TasksGroup, agentName);
                    using (var taskSchedulerAdapter = new TaskSchedulerAdapter(new RemoteMachineCredentials(agentName), TasksSettings.TasksGroup))
                    {
                        taskSchedulerAdapter.StopAllTasksFromGroup();
                    }
                }
            }
            catch (Exception e)
            {
                teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Error, "Exception while stopping tasks:\n{0}", e);
            }
        }

        private void MainProcess()
        {
            optionsSet["TestOptions"] = testOptions;
            optionsSet["TestConfiguration"] = testConfiguration;
            teamCityLogger.BeginMessageBlock("Generating child assembly");
            ChildExecutableGenerator.Generate(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "ChildExecutable"), registryCreator);
            teamCityLogger.EndMessageBlock();
            var testProgressProcessor = GetTestProgressProcessor();
            foreach (var dynamicOption in testProgressProcessor.GetDynamicOptions())
                dynamicOptionsSet[dynamicOption.Key] = dynamicOption.Value;
            var driver = new MainDriver(teamCityLogger, testConfiguration, testProgressProcessor, agentProvider);
            driver.AllProcessesStarted += onAllProcessesStarted;
            driver.Run(optionsSet, dynamicOptionsSet);
        }

        internal enum DeployPriorities
        {
            SetUpAction,
            TasksStopping,
            JmxTrans,
            Cluster,
            Driver,
            TearDownAction
        }

        private readonly Dictionary<string, object> optionsSet;
        private readonly Dictionary<string, Func<object>> dynamicOptionsSet;
        private readonly List<DeployStep> deploySteps;
        private ITeamCityLogger teamCityLogger;
        private IAgentProvider agentProvider;
        private readonly List<IDisposable> toDispose;
        private readonly TestConfiguration testConfiguration;
        private readonly MetricsContext metricsContext;
        private readonly Func<IScenariosRegistry> registryCreator;
        private readonly ITestOptions testOptions;
        private Action onAllProcessesStarted;
        private readonly List<Tuple<string, int>> innerAdditionalJmxHosts;

        internal class DeployStep
        {
            public DeployStep(string name, Action action, DeployPriorities priority)
            {
                Name = name;
                Action = action;
                Priority = priority;
            }

            public string Name { get; private set; }
            public DeployPriorities Priority { get; private set; }
            public Action Action { get; private set; }
        }
    }
}