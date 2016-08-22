using System;
using System.Collections.Generic;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.BenchmarkConfiguration.TestOptions;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.ProgressMessages;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.TestOptions;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.TestProgressProcessors;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.Tests;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.Registry
{
    public class ScenarioEssentials<TProgressMessage, TScenario, TProgressProcessor, TTestOptions> : IScenarioEssentials
        where TProgressMessage : IProgressMessage
        where TScenario : ITest<TProgressMessage, TTestOptions>
        where TProgressProcessor : ITestProgressProcessor
        where TTestOptions : ITestOptions
    {
        public ScenarioEssentials(Func<ScenarioCreationOptions, TScenario> testCreator, Func<ProgressMessageProcessorCreationOptions, TProgressProcessor> processorCreator, Func<ITestOptionsProvider, List<ITestOptions>> testOptionsCreator)
        {
            TestCreator = testCreator;
            ProcessorCreator = processorCreator;
            TestOptionsCreator = testOptionsCreator;
        }

        public Func<ScenarioCreationOptions, TScenario> TestCreator { get; private set; }
        public Func<ProgressMessageProcessorCreationOptions, TProgressProcessor> ProcessorCreator { get; private set; }
        public Func<ITestOptionsProvider, List<ITestOptions>> TestOptionsCreator { get; private set; }

        public ITest CreateTest(ScenarioCreationOptions options)
        {
            return TestCreator(options);
        }

        public ITestProgressProcessor CreateProcessor(ProgressMessageProcessorCreationOptions options)
        {
            return ProcessorCreator(options);
        }

        public List<ITestOptions> CreateTestOptions(ITestOptionsProvider testOptionsProvider)
        {
            return TestOptionsCreator(testOptionsProvider);
        }
    }
}