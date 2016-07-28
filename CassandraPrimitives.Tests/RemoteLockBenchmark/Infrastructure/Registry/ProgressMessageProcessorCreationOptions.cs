using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.TestConfigurations;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.Registry
{
    public class ProgressMessageProcessorCreationOptions
    {
        public ProgressMessageProcessorCreationOptions(TestConfiguration configuration, ITeamCityLogger teamCityLogger)
        {
            Configuration = configuration;
            TeamCityLogger = teamCityLogger;
        }

        public TestConfiguration Configuration { get; private set; }
        public ITeamCityLogger TeamCityLogger { get; private set; }
    }
}