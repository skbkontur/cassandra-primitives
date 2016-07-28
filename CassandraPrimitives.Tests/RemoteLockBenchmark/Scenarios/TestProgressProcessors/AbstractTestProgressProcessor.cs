using System;
using System.Diagnostics;

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
        protected AbstractTestProgressProcessor(TestConfiguration configuration, ITeamCityLogger teamCityLogger)
        {
            this.teamCityLogger = teamCityLogger;
            this.configuration = configuration;
            InitMetrics();
        }

        private void InitMetrics()
        {
            //TODO move to main
            Metric.SetGlobalContextName(string.Format("EDI.Benchmarks.{0}.{1}.{2}", Process.GetCurrentProcess().ProcessName.Replace('.', '_'), Environment.MachineName.Replace('.', '_'), GetTestName()));
            var metric = Metric.Config.WithHttpEndpoint("http://*:1234/").WithAllCounters();
            var graphiteUri = new Uri(string.Format("net.{0}://{1}:{2}", "tcp", "graphite-relay.skbkontur.ru", "2003"));
            Metric.Config.WithReporting(x => x.WithGraphite(graphiteUri, TimeSpan.FromSeconds(5)));

            Metric.Gauge("Progress", GetProgressInPercents, Unit.Percent);
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