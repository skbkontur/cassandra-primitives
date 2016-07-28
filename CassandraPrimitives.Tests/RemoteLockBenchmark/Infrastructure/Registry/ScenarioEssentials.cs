using System;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.ProgressMessages;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.TestProgressProcessors;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.Tests;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.Registry
{
    public class ScenarioEssentials<TProgressMessage, TScenario, TProgressProcessor> : IScenarioEssentials
        where TProgressMessage : IProgressMessage
        where TScenario : ITest<TProgressMessage>
        where TProgressProcessor : ITestProgressProcessor
    {
        public ScenarioEssentials(Func<ScenarioCreationOptions, TScenario> testCreator, Func<ProgressMessageProcessorCreationOptions, TProgressProcessor> processorCreator)
        {
            TestCreator = testCreator;
            ProcessorCreator = processorCreator;
        }

        public Func<ScenarioCreationOptions, TScenario> TestCreator { get; private set; }
        public Func<ProgressMessageProcessorCreationOptions, TProgressProcessor> ProcessorCreator { get; private set; }

        public ITest CreateTest(ScenarioCreationOptions options)
        {
            return TestCreator(options);
        }

        public ITestProgressProcessor CreateProcessor(ProgressMessageProcessorCreationOptions options)
        {
            return ProcessorCreator(options);
        }
    }
}