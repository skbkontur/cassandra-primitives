using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.TestConfigurations;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.TestProgressProcessors;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.Tests;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.Registry
{
    public interface IScenariosRegistry
    {
        ITest CreateTest(TestScenarios scenario, ScenarioCreationOptions options);
        ITestProgressProcessor CreateProcessor(TestScenarios scenario, ProgressMessageProcessorCreationOptions options);
    }
}