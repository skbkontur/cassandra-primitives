using System;
using System.Collections.Generic;

using Metrics;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.TestConfigurations;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.ProgressMessages;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.TestProgressProcessors
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
                teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "{0}%", progressPercents);
                lastReportedToTeamCityProgressPercent = progressPercents;
            }
        }

        public string HandleRawProgressMessage(string request, int processInd)
        {
            var progressMessage = JsonConvert.DeserializeObject<TProgressMessage>(request);

            return HandleProgressMessage(progressMessage, processInd);
        }

        public string HandleRawLogMessage(string request, int processInd)
        {
            var log = JObject.Parse(request);
            var message = log["message"].ToString();

            return HandleLogMessage(message, processInd);
        }

        public virtual Dictionary<string, Func<object>> GetDynamicOptions()
        {
            return new Dictionary<string, Func<object>>();
        }

        public abstract string HandleProgressMessage(TProgressMessage message, int processInd);
        public abstract string HandleLogMessage(string message, int processInd);

        protected abstract double GetProgressInPercents();

        private int lastReportedToTeamCityProgressPercent;
        protected readonly ITeamCityLogger teamCityLogger;
        protected readonly TestConfiguration configuration;
    }
}