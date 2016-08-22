using System.Collections.Generic;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.BenchmarkConfiguration.TestOptions;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.TestOptions;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.TestProgressProcessors;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.Tests;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.Registry
{
    public interface IScenariosRegistry
    {
        ITest CreateTest(string scenario, ScenarioCreationOptions options);
        ITestProgressProcessor CreateProcessor(string scenario, ProgressMessageProcessorCreationOptions options);
        List<ITestOptions> GetTestOptionsList(string scenario, ITestOptionsProvider testOptionsProvider);
    }
}