using System;
using System.Collections.Generic;
using System.IO;

using Metrics;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.CassandraInitialisation;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.BenchmarkConfiguration.TestOptions;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Implementations.ZooKeeper.ZookeeperSettings;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.Agents.Providers;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.Registry;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.TestConfigurations;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.TestOptions;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.SchemeActualizer;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.BenchmarkConfiguration
{
    public class ManyOptionsBenchmarkConfigurator : IBenchmarkConfigurator
    {
        private ManyOptionsBenchmarkConfigurator(List<TestConfiguration> testConfigurationsList, Func<IScenariosRegistry> staticRegistryCreatorMethod, IEnvironmentVariableProvider variableProvider)
        {
            this.testConfigurationsList = testConfigurationsList;
            this.staticRegistryCreatorMethod = staticRegistryCreatorMethod;
            this.variableProvider = variableProvider;
            teamCityLogger = new FakeTeamCityLogger();
            scenariosRegistry = staticRegistryCreatorMethod();
            actions = new List<Action<IBenchmarkConfigurator>>();
        }

        public static IBenchmarkConfigurator CreateNew(List<TestConfiguration> testConfigurationsList, Func<IScenariosRegistry> staticRegistryCreatorMethod, IEnvironmentVariableProvider variableProvider)
        {
            return new ManyOptionsBenchmarkConfigurator(testConfigurationsList, staticRegistryCreatorMethod, variableProvider);
        }

        public static IBenchmarkConfigurator CreateNew(IEnvironmentVariableProvider variableProvider, Func<IScenariosRegistry> staticRegistryCreatorMethod)
        {
            if (!staticRegistryCreatorMethod.Method.IsStatic ||
                !staticRegistryCreatorMethod.Method.IsPublic ||
                staticRegistryCreatorMethod.Method.DeclaringType == null ||
                !staticRegistryCreatorMethod.Method.DeclaringType.IsPublic)
                throw new Exception("Invalid staticRegistryCreatorMethod. It should be static, public and defined in a public class");
            var testConfigurationsList = TestConfiguration.ParseWithRanges(TestEnvironment.GetFromEnvironment(variableProvider));
            return new ManyOptionsBenchmarkConfigurator(testConfigurationsList, staticRegistryCreatorMethod, variableProvider);
        }

        public void StartAndWaitForFinish()
        {
            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Going to run {0} test configuration(s)", testConfigurationsList.Count);
            var testOptionsProvider = new TestOptionsProvider(variableProvider);
            for (int testConfigurationInd = 0; testConfigurationInd < testConfigurationsList.Count; testConfigurationInd++)
            {
                var testConfiguration = testConfigurationsList[testConfigurationInd];
                var testOptionsList = scenariosRegistry.GetTestOptionsList(testConfiguration.TestScenario, testOptionsProvider);
                for (int testOptionsInd = 0; testOptionsInd < testOptionsList.Count; testOptionsInd++)
                {
                    var testOptions = testOptionsList[testOptionsInd];
                    Start(testConfiguration, testConfigurationInd, testOptions, testOptionsInd, testOptionsList.Count);
                }
            }
        }

        private void Start(TestConfiguration testConfiguration, int configurationInd, ITestOptions testOptions, int optionsInd, int totalOptionCount)
        {
            var blockName = string.Format("Configuration - {0}/{1}, options - {2}/{3}", configurationInd + 1, testConfigurationsList.Count, optionsInd + 1, totalOptionCount);
            using (teamCityLogger.MessageBlock(blockName))
            using (teamCityLogger.Activity(blockName))
            {
                teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Configuration:\n{0}", testConfiguration);
                teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Options:\n{0}", testOptions);
                var metricsContextName = string.Format("Test configuration - {0}, options set - {1}", configurationInd, optionsInd);
                
                new AnnotationsPublisher().PublishAnnotation(string.Format("Benchmark start\n\nConfiguration:\n{0}\nOptions:\n{1}", testConfiguration, testOptions), "edi_benchmarks");

                var currentArtifactsDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "CurrentArtifacts");
                var artifactsDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Artifacts");

                using (new ArtifactsPublisher(currentArtifactsDir, artifactsDir, string.Format("Config_{0}_Options_{1}", configurationInd, optionsInd)))
                {
                    try
                    {
                        var metricsContext = Metric.Context(metricsContextName);
                        var configurator = SingleRunBenchmarkConfigurator.CreateNew(testConfiguration, testOptions, staticRegistryCreatorMethod, metricsContext);
                        foreach (var action in actions)
                        {
                            action(configurator);
                        }
                        configurator.StartAndWaitForFinish();
                    }
                    catch (Exception e)
                    {
                        teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Error, "Exception occured while running test, stopping. Exception:\n{0}", e);
                    }
                    finally
                    {
                        Metric.ShutdownContext(metricsContextName);
                    }
                }
            }
        }

        public IReadyToStartBenchmarkConfigurator WithDefaultTeamCityLogger()
        {
            teamCityLogger = new TeamCityLogger(Console.Out);
            actions.Add(benchmarkConfigurator => benchmarkConfigurator.WithTeamCityLogger(teamCityLogger));
            return this;
        }

        public IReadyToStartBenchmarkConfigurator WithTeamCityLogger(ITeamCityLogger teamCityLogger)
        {
            this.teamCityLogger = teamCityLogger;
            actions.Add(benchmarkConfigurator => benchmarkConfigurator.WithTeamCityLogger(teamCityLogger));
            return this;
        }

        private readonly List<Action<IBenchmarkConfigurator>> actions;
        private readonly List<TestConfiguration> testConfigurationsList;
        private readonly IScenariosRegistry scenariosRegistry;
        private readonly Func<IScenariosRegistry> staticRegistryCreatorMethod;
        private readonly IEnvironmentVariableProvider variableProvider;
        private ITeamCityLogger teamCityLogger;

        #region JustDelegationToSingleRunBenchmarkConfigurator

        public IReadyToStartBenchmarkConfigurator WithAgentProvider(IAgentProvider agentProvider)
        {
            actions.Add(benchmarkConfigurator => benchmarkConfigurator.WithAgentProvider(agentProvider));
            return this;
        }

        public IReadyToStartBenchmarkConfigurator WithAgentProviderFromTeamCity()
        {
            actions.Add(benchmarkConfigurator => benchmarkConfigurator.WithAgentProviderFromTeamCity(variableProvider));
            return this;
        }

        public IReadyToStartBenchmarkConfigurator WithAgentProviderFromTeamCity(IEnvironmentVariableProvider variableProvider)
        {
            actions.Add(benchmarkConfigurator => benchmarkConfigurator.WithAgentProviderFromTeamCity(variableProvider));
            return this;
        }

        public IReadyToStartBenchmarkConfigurator WithCassandraCluster(ICassandraMetadataProvider cassandraMetadataProvider)
        {
            actions.Add(benchmarkConfigurator => benchmarkConfigurator.WithCassandraCluster(cassandraMetadataProvider));
            return this;
        }

        public IReadyToStartBenchmarkConfigurator WithExistingCassandraCluster(CassandraClusterSettings clusterSettings, ICassandraMetadataProvider cassandraMetadataProvider)
        {
            actions.Add(benchmarkConfigurator => benchmarkConfigurator.WithExistingCassandraCluster(clusterSettings, cassandraMetadataProvider));
            return this;
        }

        public IReadyToStartBenchmarkConfigurator WithZookeeperCluster()
        {
            actions.Add(benchmarkConfigurator => benchmarkConfigurator.WithZookeeperCluster());
            return this;
        }

        public IReadyToStartBenchmarkConfigurator WithExistingZookeeperCluster(ZookeeperClusterSettings clusterSettings)
        {
            actions.Add(benchmarkConfigurator => benchmarkConfigurator.WithExistingZookeeperCluster(clusterSettings));
            return this;
        }

        public IReadyToStartBenchmarkConfigurator WithClusterFromConfiguration(ICassandraMetadataProvider cassandraMetadataProvider)
        {
            actions.Add(benchmarkConfigurator => benchmarkConfigurator.WithClusterFromConfiguration(cassandraMetadataProvider));
            return this;
        }

        public IReadyToStartBenchmarkConfigurator WithOption(string name, object value)
        {
            actions.Add(benchmarkConfigurator => benchmarkConfigurator.WithOption(name, value));
            return this;
        }

        public IReadyToStartBenchmarkConfigurator WithDynamicOption(string name, Func<object> valueProvider)
        {
            actions.Add(benchmarkConfigurator => benchmarkConfigurator.WithDynamicOption(name, valueProvider));
            return this;
        }

        public IReadyToStartBenchmarkConfigurator WithAllProcessStartedHandler(Action onAllProcessesStarted)
        {
            actions.Add(benchmarkConfigurator => benchmarkConfigurator.WithAllProcessStartedHandler(onAllProcessesStarted));
            return this;
        }

        public IReadyToStartBenchmarkConfigurator WithJmxTrans(string graphitePrefix)
        {
            actions.Add(benchmarkConfigurator => benchmarkConfigurator.WithJmxTrans(graphitePrefix));
            return this;
        }

        public IReadyToStartBenchmarkConfigurator WithJmxTrans(string graphitePrefix, Tuple<string, int>[] additionalJmxEndPoints)
        {
            actions.Add(benchmarkConfigurator => benchmarkConfigurator.WithJmxTrans(graphitePrefix, additionalJmxEndPoints));
            return this;
        }

        public IReadyToStartBenchmarkConfigurator WithSetUpAction(Action action)
        {
            actions.Add(benchmarkConfigurator => benchmarkConfigurator.WithSetUpAction(action));
            return this;
        }

        public IReadyToStartBenchmarkConfigurator WithTearDownAction(Action action)
        {
            actions.Add(benchmarkConfigurator => benchmarkConfigurator.WithTearDownAction(action));
            return this;
        }

        #endregion
    }
}