using Metrics;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.TestConfigurations;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.Registry
{
    public class ProgressMessageProcessorCreationOptions
    {
        public ProgressMessageProcessorCreationOptions(TestConfiguration configuration, ITeamCityLogger teamCityLogger, MetricsContext metricsContext)
        {
            Configuration = configuration;
            TeamCityLogger = teamCityLogger;
            MetricsContext = metricsContext;
        }

        public TestConfiguration Configuration { get; private set; }
        public ITeamCityLogger TeamCityLogger { get; private set; }
        public MetricsContext MetricsContext { get; private set; }
    }
}