using System;
using System.Collections.Generic;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.TestConfigurations;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.ProgressMessages;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.TestProgressProcessors;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.Tests;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.Registry
{
    public class ScenariosRegistry : IScenariosRegistry
    {
        public ScenariosRegistry()
        {
            scenarios = new Dictionary<TestScenarios, IScenarioEssentials>();
        }

        public ITest CreateTest(TestScenarios scenario, ScenarioCreationOptions options)
        {
            return GetScenario(scenario).CreateTest(options);
        }

        public ITestProgressProcessor CreateProcessor(TestScenarios scenario, ProgressMessageProcessorCreationOptions options)
        {
            return GetScenario(scenario).CreateProcessor(options);
        }

        private IScenarioEssentials GetScenario(TestScenarios scenario)
        {
            if (!scenarios.ContainsKey(scenario))
                throw new Exception(string.Format("Unknown scenario {0}", scenario));
            return scenarios[scenario];
        }

        public void Register<TProgressMessage, TScenario, TProgressProcessor>(TestScenarios scenario, Func<ScenarioCreationOptions, TScenario> testCreator, Func<ProgressMessageProcessorCreationOptions, TProgressProcessor> processorCreator)
            where TProgressMessage : IProgressMessage
            where TScenario : ITest<TProgressMessage>
            where TProgressProcessor : ITestProgressProcessor
        {
            scenarios[scenario] = new ScenarioEssentials<TProgressMessage, TScenario, TProgressProcessor>(testCreator, processorCreator);
        }

        private readonly Dictionary<TestScenarios, IScenarioEssentials> scenarios;
    }
}