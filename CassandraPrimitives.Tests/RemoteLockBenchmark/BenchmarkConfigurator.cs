using System;
using System.Collections.Generic;
using System.Linq;

using log4net;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.Agents.Providers;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.ChildProcessDriver;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.ExternalLogging.HttpLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.MainDriver;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.TestConfigurations;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.TestProgressProcessors;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark
{
    public class BenchmarkConfigurator
    {
        private BenchmarkConfigurator()
        {
            deploySteps = new List<DeployStep>();
            toDispose = new List<IDisposable>();
            optionsSet = new Dictionary<string, object>();
        }

        public static BenchmarkConfigurator Configure()
        {
            return new BenchmarkConfigurator();
        }

        public BenchmarkConfigurator NoDeploy()
        {
            noDeploy = true;
            return this;
        }

        public BenchmarkConfigurator WithTeamCityLogger(ITeamCityLogger teamCityLogger)
        {
            this.teamCityLogger = teamCityLogger;
            return this;
        }

        public BenchmarkConfigurator WithTeamCityLogger()
        {
            teamCityLogger = new TeamCityLogger(Console.Out);
            return this;
        }

        public BenchmarkConfigurator WithAgentProvider(IAgentProvider agentProvider)
        {
            this.agentProvider = agentProvider;
            return this;
        }

        public BenchmarkConfigurator WithAgentProviderFromTeamCity()
        {
            agentProvider = new AgentProviderFromTeamCity();
            return this;
        }

        public BenchmarkConfigurator WithConfiguration(TestConfiguration testConfiguration)
        {
            this.testConfiguration = testConfiguration;
            this.optionsSet["TestConfiguration"] = testConfiguration;
            return this;
        }

        public BenchmarkConfigurator WithConfigurationFromTeamCity()
        {
            deploySteps.Add(new DeployStep("Get configuration", () =>
                {
                    testConfiguration = TestConfiguration.GetFromEnvironment();
                    optionsSet["TestConfiguration"] = testConfiguration;
                }, DeployPriorities.Configuration));
            return this;
        }

        public BenchmarkConfigurator WithCassandraCluster()
        {
            deploySteps.Add(new DeployStep("Cassandra deploy", () =>
                {
                    var cassandraAgents = agentProvider.AcquireAgents(testConfiguration.AmountOfClusterNodes);
                    var wrapperDeployer = new WrapperDeployer(teamCityLogger, noDeploy);
                    wrapperDeployer.DeployWrapperToAgents(cassandraAgents);
                    var cassandraDriver = new CassandraMainDriver(teamCityLogger, cassandraAgents, wrapperDeployer.GetWrapperRelativePath(), noDeploy);
                    toDispose.Add(cassandraDriver.StartCassandraCluster());
                    optionsSet["CassandraClusterSettings"] = cassandraDriver.ClusterSettings;
                }, DeployPriorities.Cluster));
            return this;
        }

        public BenchmarkConfigurator WithZookeeperCluster()
        {
            deploySteps.Add(new DeployStep("Zookeeper deploy", () =>
                {
                    var zookeeperAgents = agentProvider.AcquireAgents(testConfiguration.AmountOfClusterNodes);
                    var wrapperDeployer = new WrapperDeployer(teamCityLogger, noDeploy);
                    wrapperDeployer.DeployWrapperToAgents(zookeeperAgents);
                    var zookeeperDriver = new ZookeeperMainDriver(teamCityLogger, zookeeperAgents, wrapperDeployer.GetWrapperRelativePath(), noDeploy);
                    toDispose.Add(zookeeperDriver.StartZookeeperCluster());
                    optionsSet["ZookeeperClusterSettings"] = zookeeperDriver.ClusterSettings;
                }, DeployPriorities.Cluster));
            return this;
        }

        public void Start()
        {
            try
            {
                var args = Environment.GetCommandLineArgs();
                if (args.Length < 2 || args[1] != ConstantBenchmarkToken)
                {
                    deploySteps.Add(new DeployStep("MainProcess", MainProcess, DeployPriorities.Driver));
                    foreach (var deployStep in deploySteps.OrderBy(s => s.Priority))
                    {
                        teamCityLogger.BeginMessageBlock(deployStep.Name);
                        deployStep.Action.Invoke();
                        teamCityLogger.EndMessageBlock();
                    }
                }
                else
                    ChildProcess(args.Skip(2).ToArray());
            }
            finally
            {
                foreach (var disposable in toDispose)
                    disposable.Dispose();
            }
        }

        private ITestProgressProcessor GetTestProgressProcessor()
        {
            switch (testConfiguration.TestScenario)
            {
                case TestScenarios.Timeline:
                    var progressProcessor = new TimelineTestProgressProcessor(testConfiguration, teamCityLogger);
                    toDispose.Add(progressProcessor);
                    return progressProcessor;
                default:
                throw new Exception(string.Format("Unknown TestScenario {0}", testConfiguration.TestScenario));
            }
        }

        private void MainProcess()
        {
            optionsSet["LockId"] = Guid.NewGuid().ToString();
            var testProgressProcessor = GetTestProgressProcessor();
            var driver = new MainDriver(teamCityLogger, testConfiguration, testProgressProcessor, agentProvider, noDeploy);
            driver.Run(optionsSet);
        }

        private void ChildProcess(string[] args)
        {
            var logger = LogManager.GetLogger(GetType());
            if (args.Length < 3)
                throw new Exception("Not enough arguments");

            int processInd;
            if (!int.TryParse(args[0], out processInd))
                throw new Exception(string.Format("Invalid process id {0}", args[0]));

            logger.InfoFormat("Process id is {0}", processInd);
            logger.InfoFormat("Remote http address is {0}", args[1]);

            var processToken = args[2];

            TestConfiguration configuration;
            using (var httpExternalDataGetter = new HttpExternalDataGetter(args[1], 12345))
                configuration = httpExternalDataGetter.GetTestConfiguration().Result;
            logger.InfoFormat("Configuration was received");

            ChildProcessDriver.RunSingleTest(configuration, processInd, processToken);
        }

        public const string ConstantBenchmarkToken = "constant-benchmark-token-f6718f48-0cc5-4f20-aec6-102d9fa09635";

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
        private bool noDeploy;
        private TestConfiguration testConfiguration;

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