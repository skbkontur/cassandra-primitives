using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.TestProgressProcessors;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.Tests;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.Registry
{
    public interface IScenarioEssentials
    {
        ITest CreateTest(ScenarioCreationOptions options);
        ITestProgressProcessor CreateProcessor(ProgressMessageProcessorCreationOptions options);
    }
}