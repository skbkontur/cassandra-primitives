using System;
using System.Collections.Generic;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.BenchmarkConfiguration.TestOptions;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.ProgressMessages;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.TestOptions;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.TestProgressProcessors;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.Tests;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.Registry
{
    public class ScenariosRegistry : IScenariosRegistry
    {
        public ScenariosRegistry()
        {
            scenarioEssentials = new Dictionary<string, IScenarioEssentials>();
        }

        public ITest CreateTest(string scenario, ScenarioCreationOptions options)
        {
            return GetScenarioEssentials(scenario).CreateTest(options);
        }

        public ITestProgressProcessor CreateProcessor(string scenario, ProgressMessageProcessorCreationOptions options)
        {
            return GetScenarioEssentials(scenario).CreateProcessor(options);
        }

        public List<ITestOptions> GetTestOptionsList(string scenario, ITestOptionsProvider testOptionsProvider)
        {
            return GetScenarioEssentials(scenario).CreateTestOptions(testOptionsProvider);
        }

        private IScenarioEssentials GetScenarioEssentials(string scenario)
        {
            if (!scenarioEssentials.ContainsKey(scenario))
                throw new Exception(string.Format("Unknown scenario {0}", scenario));
            return scenarioEssentials[scenario];
        }

        public void Register<TProgressMessage, TScenario, TProgressProcessor, TTestOptions>(string scenario, Func<ScenarioCreationOptions, TScenario> testCreator, Func<ProgressMessageProcessorCreationOptions, TProgressProcessor> processorCreator, Func<ITestOptionsProvider, List<ITestOptions>> testOptionsCreator)
            where TProgressMessage : IProgressMessage
            where TScenario : ITest<TProgressMessage, TTestOptions>
            where TProgressProcessor : ITestProgressProcessor
            where TTestOptions : ITestOptions
        {
            scenarioEssentials[scenario] = new ScenarioEssentials<TProgressMessage, TScenario, TProgressProcessor, TTestOptions>(testCreator, processorCreator, testOptionsCreator);
        }

        private readonly Dictionary<string, IScenarioEssentials> scenarioEssentials;
    }
}