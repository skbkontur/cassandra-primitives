using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.ExternalLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.ExternalLogging.HttpLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.TestConfigurations;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.Registry
{
    public class ScenarioCreationOptions
    {
        public ScenarioCreationOptions(TestConfiguration configuration, int processInd, string processToken, IExternalProgressLogger externalProgressLogger, HttpExternalDataGetter externalDataGetter)
        {
            Configuration = configuration;
            ProcessInd = processInd;
            ProcessToken = processToken;
            ExternalProgressLogger = externalProgressLogger;
            ExternalDataGetter = externalDataGetter;
        }

        public TestConfiguration Configuration { get; private set; }
        public int ProcessInd { get; private set; }
        public string ProcessToken { get; private set; }

        public IExternalProgressLogger ExternalProgressLogger { get; private set; }
        public HttpExternalDataGetter ExternalDataGetter { get; private set; }
    }
}