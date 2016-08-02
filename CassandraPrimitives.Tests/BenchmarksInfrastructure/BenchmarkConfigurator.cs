using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

using Metrics;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.Agents.Providers;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.ChildProcessDriver;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.MainDriver;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.Registry;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.TestConfigurations;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.TestOptions;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.TestProgressProcessors;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure
{
    public interface IWaitingForRegistryCreatorBenchmarkConfigurator
    {
        IWaitingForAgentProviderBenchmarkConfigurator WithStaticRegistryCreatorMethod(Func<IScenariosRegistry> staticRegistryCreatorMethod);
    }

    public interface IWaitingForAgentProviderBenchmarkConfigurator
    {
        IWaitingForTestConfigurationBenchmarkConfigurator WithAgentProvider(IAgentProvider agentProvider);
        IWaitingForTestConfigurationBenchmarkConfigurator WithAgentProviderFromTeamCity();
    }

    public interface IWaitingForTestConfigurationBenchmarkConfigurator
    {
        IWaitingForTestOptionsBenchmarkConfigurator WithConfiguration(TestConfiguration testConfiguration);
    }

    public interface IWaitingForTestOptionsBenchmarkConfigurator
    {
        IReadyToStartBenchmarkConfigurator WithTestOptions(ITestOptions testOptions);
    }

    public interface IReadyToStartBenchmarkConfigurator
    {
        IReadyToStartBenchmarkConfigurator WithCassandraCluster();
        IReadyToStartBenchmarkConfigurator WithZookeeperCluster();
        IReadyToStartBenchmarkConfigurator WithClusterFromConfiguration();
        IReadyToStartBenchmarkConfigurator WithMetricsContext(MetricsContext context);
        IReadyToStartBenchmarkConfigurator WithDefaultTeamCityLogger();
        IReadyToStartBenchmarkConfigurator WithTeamCityLogger(ITeamCityLogger teamCityLogger);
        void StartAndWaitForFinish();
    }

    public class BenchmarkConfigurator :
        IWaitingForRegistryCreatorBenchmarkConfigurator,
        IWaitingForAgentProviderBenchmarkConfigurator,
        IWaitingForTestConfigurationBenchmarkConfigurator,
        IWaitingForTestOptionsBenchmarkConfigurator,
        IReadyToStartBenchmarkConfigurator
    {
        private BenchmarkConfigurator()
        {
            deploySteps = new List<DeployStep>();
            teamCityLogger = new FakeTeamCityLogger();
            toDispose = new List<IDisposable>();
            optionsSet = new Dictionary<string, object>();
        }

        public static IWaitingForRegistryCreatorBenchmarkConfigurator CreateNew()
        {
            return new BenchmarkConfigurator();
        }

        public IWaitingForAgentProviderBenchmarkConfigurator WithStaticRegistryCreatorMethod(Func<IScenariosRegistry> staticRegistryCreatorMethod)
        {
            registryCreator = staticRegistryCreatorMethod;
            return this;
        }

        public IReadyToStartBenchmarkConfigurator WithTeamCityLogger(ITeamCityLogger teamCityLogger)
        {
            this.teamCityLogger = teamCityLogger;
            return this;
        }

        public IReadyToStartBenchmarkConfigurator WithDefaultTeamCityLogger()
        {
            teamCityLogger = new TeamCityLogger(Console.Out);
            return this;
        }

        public BenchmarkConfigurator WithTeamCityLogger()
        {
            teamCityLogger = new TeamCityLogger(Console.Out);
            return this;
        }

        public IWaitingForTestConfigurationBenchmarkConfigurator WithAgentProvider(IAgentProvider agentProvider)
        {
            this.agentProvider = agentProvider;
            return this;
        }

        public IWaitingForTestConfigurationBenchmarkConfigurator WithAgentProviderFromTeamCity()
        {
            agentProvider = new AgentProviderFromTeamCity();
            return this;
        }

        public IWaitingForTestOptionsBenchmarkConfigurator WithConfiguration(TestConfiguration testConfiguration)
        {
            this.testConfiguration = testConfiguration;
            optionsSet["TestConfiguration"] = testConfiguration;
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

        public IReadyToStartBenchmarkConfigurator WithMetricsContext(MetricsContext context)
        {
            metricsContext = context;
            return this;
        }

        public IReadyToStartBenchmarkConfigurator WithTestOptions(ITestOptions testOptions)
        {
            this.testOptions = testOptions;
            return this;
        }

        public void StartAndWaitForFinish()
        {
            try
            {
                deploySteps.Add(new DeployStep("MainProcess", MainProcess, DeployPriorities.Driver));
                foreach (var deployStep in deploySteps.OrderBy(s => s.Priority))
                {
                    teamCityLogger.BeginMessageBlock(deployStep.Name);
                    deployStep.Action.Invoke();
                    teamCityLogger.EndMessageBlock();
                }
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

        private void MainProcess()
        {
            optionsSet["LockId"] = Guid.NewGuid().ToString();
            optionsSet["TestOptions"] = testOptions;
            teamCityLogger.BeginMessageBlock("Generating child assembly");
            ChildExecutableGenerator.Generate(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "ChildExecutable"), registryCreator);
            teamCityLogger.EndMessageBlock();
            var testProgressProcessor = GetTestProgressProcessor();
            var driver = new MainDriver(teamCityLogger, testConfiguration, testProgressProcessor, agentProvider);
            driver.Run(optionsSet);
        }

        internal enum DeployPriorities
        {
            Configuration,
            Cluster,
            Driver
        }

        private readonly Dictionary<string, object> optionsSet;
        private readonly List<DeployStep> deploySteps;
        private ITeamCityLogger teamCityLogger;
        private IAgentProvider agentProvider;
        private readonly List<IDisposable> toDispose;
        private TestConfiguration testConfiguration;
        private MetricsContext metricsContext;
        private Func<IScenariosRegistry> registryCreator;
        private ITestOptions testOptions;

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