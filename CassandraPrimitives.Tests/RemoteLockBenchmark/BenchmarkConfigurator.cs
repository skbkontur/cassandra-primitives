using System;
using System.Collections.Generic;
using System.Linq;

using log4net;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.Agents;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.ChildProcessDriver;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.ExternalLogging.HttpLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.MainDriver;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.TestConfigurations;
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

        public BenchmarkConfigurator WithAgentProvider(IAgentProvider agentProvider)
        {
            this.agentProvider = agentProvider;
            return this;
        }

        public BenchmarkConfigurator WithConfiguration(TestConfiguration testConfiguration)
        {
            this.testConfiguration = testConfiguration;
            this.optionsSet["TestConfiguration"] = testConfiguration;
            return this;
        }

        public BenchmarkConfigurator WithCassandraCluster(int amountOfNodes)
        {
            deploySteps.Add(new DeployStep("Cassandra deploy", () =>
                {
                    var cassandraAgents = agentProvider.AcquireAgents(amountOfNodes);
                    new WrapperDeployer(teamCityLogger, noDeploy).DeployWrapperToAgents(cassandraAgents);
                    var cassandraDriver = new CassandraMainDriver(teamCityLogger, cassandraAgents, noDeploy);
                    toDispose.Add(cassandraDriver.StartCassandraCluster());
                    optionsSet["CassandraClusterSettings"] = cassandraDriver.ClusterSettings;
                }, 1));
            return this;
        }

        public BenchmarkConfigurator WithZookeeperCluster(int amountOfNodes)
        {
            deploySteps.Add(new DeployStep("Zookeeper deploy", () =>
                {
                    var zookeeperAgents = agentProvider.AcquireAgents(amountOfNodes);
                    new WrapperDeployer(teamCityLogger, noDeploy).DeployWrapperToAgents(zookeeperAgents);
                    var zookeeperDriver = new ZookeeperMainDriver(teamCityLogger, zookeeperAgents, noDeploy);
                    toDispose.Add(zookeeperDriver.StartZookeeperCluster());
                    optionsSet["ZookeeperClusterSettings"] = zookeeperDriver.ClusterSettings;
                }, 1));
            return this;
        }

        public BenchmarkConfigurator WithTest<TTest>()
        {
            testType = typeof(TTest);
            return this;
        }

        public void Start()
        {
            try
            {
                var args = Environment.GetCommandLineArgs().Skip(1).ToArray();
                if (args.Length == 0)
                {
                    deploySteps.Add(new DeployStep("MainProcess", MainProcess, 2));
                    foreach (var deployStep in deploySteps.OrderBy(s => s.Priority))
                    {
                        teamCityLogger.BeginMessageBlock(deployStep.Name);
                        deployStep.Action.Invoke();
                        teamCityLogger.EndMessageBlock();
                    }
                }
                else
                    ChildProcess(args);
            }
            finally
            {
                foreach (var disposable in toDispose)
                    disposable.Dispose();
            }
        }

        private void MainProcess()
        {
            optionsSet["LockId"] = Guid.NewGuid().ToString();
            var driver = new MainDriver(teamCityLogger, testConfiguration, agentProvider, noDeploy);
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

            ChildProcessDriver.RunSingleTest(configuration, processInd, processToken, testType);
        }

        private readonly Dictionary<string, object> optionsSet;
        private readonly List<DeployStep> deploySteps;
        private ITeamCityLogger teamCityLogger;
        private IAgentProvider agentProvider;
        private readonly List<IDisposable> toDispose;
        private bool noDeploy;
        private TestConfiguration testConfiguration;
        private Type testType;

        internal class DeployStep
        {
            public DeployStep(string name, Action action, int priority)
            {
                Name = name;
                Action = action;
                Priority = priority;
            }

            public string Name { get; private set; }
            public int Priority { get; private set; }
            public Action Action { get; private set; }
        }
    }
}