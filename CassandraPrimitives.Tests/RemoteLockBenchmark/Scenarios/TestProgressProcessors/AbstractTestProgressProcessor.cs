using Metrics;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.TestConfigurations;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.ProgressMessages;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.TestProgressProcessors
{
    public abstract class AbstractTestProgressProcessor<TProgressMessage> : ITestProgressProcessor
        where TProgressMessage : IProgressMessage
    {
        protected AbstractTestProgressProcessor(TestConfiguration configuration, ITeamCityLogger teamCityLogger, MetricsContext metricsContext)
        {
            this.teamCityLogger = teamCityLogger;
            this.configuration = configuration;
            InitMetrics(metricsContext);
        }

        private void InitMetrics(MetricsContext metricsContext)
        {
            metricsContext.Gauge("Progress", GetProgressInPercents, Unit.Percent);
        }

        protected abstract string GetTestName();

        protected void ReportProgressToTeamCity()
        {
            var progressPercents = (int)GetProgressInPercents();
            if (lastReportedToTeamCityProgressPercent != progressPercents)
            {
                teamCityLogger.ReportActivity(string.Format("{0}%", progressPercents));
                lastReportedToTeamCityProgressPercent = progressPercents;
            }
        }

        public string HandlePublishProgress(string request, int processInd)
        {
            var progressMessage = JsonConvert.DeserializeObject<TProgressMessage>(request);

            return HandlePublishProgress(progressMessage, processInd);
        }

        public string HandleLog(string request, int processInd)
        {
            var log = JObject.Parse(request);
            var message = log["message"].ToString();

            return HandleLogMessage(message, processInd);
        }



        public abstract string HandlePublishProgress(TProgressMessage message, int processInd);
        public abstract string HandleLogMessage(string message, int processInd);

        protected abstract double GetProgressInPercents();

        private int lastReportedToTeamCityProgressPercent;
        protected readonly ITeamCityLogger teamCityLogger;
        protected readonly TestConfiguration configuration;
    }
}