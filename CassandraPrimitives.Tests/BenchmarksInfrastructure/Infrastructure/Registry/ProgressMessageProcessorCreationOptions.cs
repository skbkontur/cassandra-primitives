using Metrics;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.TestConfigurations;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.TestOptions;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.Registry
{
    public class ProgressMessageProcessorCreationOptions
    {
        public ProgressMessageProcessorCreationOptions(TestConfiguration configuration, ITeamCityLogger teamCityLogger, MetricsContext metricsContext, ITestOptions testOptions)
        {
            Configuration = configuration;
            TeamCityLogger = teamCityLogger;
            MetricsContext = metricsContext;
            TestOptions = testOptions;
        }

        public TestConfiguration Configuration { get; private set; }
        public ITeamCityLogger TeamCityLogger { get; private set; }
        public MetricsContext MetricsContext { get; private set; }
        public ITestOptions TestOptions { get; private set; }
    }
}