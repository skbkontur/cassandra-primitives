using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.TestProgressProcessors;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.Tests;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.Registry
{
    public interface IScenarioEssentials
    {
        ITest CreateTest(ScenarioCreationOptions options);
        ITestProgressProcessor CreateProcessor(ProgressMessageProcessorCreationOptions options);
    }
}