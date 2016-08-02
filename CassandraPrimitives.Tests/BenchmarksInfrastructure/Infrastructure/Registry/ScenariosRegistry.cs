using System;
using System.Collections.Generic;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.TestConfigurations;
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
            scenarioEssentials = new Dictionary<TestScenarios, IScenarioEssentials>();
        }

        public ITest CreateTest(TestScenarios scenario, ScenarioCreationOptions options)
        {
            return GetScenarioEssentials(scenario).CreateTest(options);
        }

        public ITestProgressProcessor CreateProcessor(TestScenarios scenario, ProgressMessageProcessorCreationOptions options)
        {
            return GetScenarioEssentials(scenario).CreateProcessor(options);
        }

        private IScenarioEssentials GetScenarioEssentials(TestScenarios scenario)
        {
            if (!scenarioEssentials.ContainsKey(scenario))
                throw new Exception(string.Format("Unknown scenario {0}", scenario));
            return scenarioEssentials[scenario];
        }

        public void Register<TProgressMessage, TScenario, TProgressProcessor, TTestOptions>(TestScenarios scenario, Func<ScenarioCreationOptions, TScenario> testCreator, Func<ProgressMessageProcessorCreationOptions, TProgressProcessor> processorCreator)
            where TProgressMessage : IProgressMessage
            where TScenario : ITest<TProgressMessage, TTestOptions>
            where TProgressProcessor : ITestProgressProcessor
            where TTestOptions : ITestOptions
        {
            scenarioEssentials[scenario] = new ScenarioEssentials<TProgressMessage, TScenario, TProgressProcessor, TTestOptions>(testCreator, processorCreator);
        }

        private readonly Dictionary<TestScenarios, IScenarioEssentials> scenarioEssentials;
    }
}