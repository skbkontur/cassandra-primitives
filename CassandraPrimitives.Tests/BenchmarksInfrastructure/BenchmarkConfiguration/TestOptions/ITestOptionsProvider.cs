using System.Collections.Generic;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.TestOptions;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.BenchmarkConfiguration.TestOptions
{
    public interface ITestOptionsProvider
    {
        List<TTestOptions> CreateOptions<TTestOptions>()
            where TTestOptions : ITestOptions, new();
    }
}