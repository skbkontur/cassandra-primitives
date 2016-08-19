using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

using Metrics;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.JmxInitialisation;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.RemoteTaskRunning;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Implementations.Cassandra.CassandraSettings;
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

        public IReadyToStartBenchmarkConfigurator WithCassandraCluster()
        {
            deploySteps.Add(new DeployStep("Cassandra deploy", () =>
                {
                    var cassandraAgents = agentProvider.AcquireAgents(testConfiguration.AmountOfClusterNodes);
                    var wrapperDeployer = new WrapperDeployer(teamCityLogger);
                    wrapperDeployer.DeployWrapperToAgents(cassandraAgents);
                    var cassandraDriver = new CassandraMainDriver(teamCityLogger, cassandraAgents, wrapperDeployer.GetWrapperRelativePath());
                    toDispose.Add(cassandraDriver.StartCassandraCluster());
                    optionsSet["CassandraClusterSettings"] = cassandraDriver.ClusterSettings;
                }, DeployPriorities.Cluster));
            return this;
        }

        public IReadyToStartBenchmarkConfigurator WithExistingCassandraCluster(CassandraClusterSettings clusterSettings)
        {
            deploySteps.Add(new DeployStep("Configure cassandra cluster", () =>
                {
                    using (var cassandraCluster = new CassandraCluster(clusterSettings))
                    {
                        var initializerSettings = new CassandraInitializerSettings();
                        var cassandraSchemeActualizer = new CassandraSchemeActualizer(cassandraCluster, new CassandraMetaProvider(), initializerSettings);
                        cassandraSchemeActualizer.AddNewColumnFamilies();
                    }
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

        public IReadyToStartBenchmarkConfigurator WithClusterFromConfiguration()
        {
            switch (testConfiguration.ClusterType)
            {
            case ClusterTypes.Cassandra:
                return WithCassandraCluster();
            case ClusterTypes.Zookeeper:
                return WithZookeeperCluster();
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