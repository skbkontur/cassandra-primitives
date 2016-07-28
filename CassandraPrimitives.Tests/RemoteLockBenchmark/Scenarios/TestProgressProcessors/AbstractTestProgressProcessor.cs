using System;
using System.Diagnostics;

using Metrics;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.TestConfigurations;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.TestProgressProcessors
{
    public abstract class AbstractTestProgressProcessor : ITestProgressProcessor, IDisposable
    {
        protected AbstractTestProgressProcessor(TestConfiguration configuration, ITeamCityLogger teamCityLogger)
        {
            this.teamCityLogger = teamCityLogger;
            this.configuration = configuration;
            InitMetrics();
        }

        private void InitMetrics()
        {
            Metric.SetGlobalContextName(string.Format("EDI.Benchmarks.{0}.{1}.{2}", Process.GetCurrentProcess().ProcessName.Replace('.', '_'), Environment.MachineName.Replace('.', '_'), GetTestName()));
            metric = Metric.Config.WithHttpEndpoint("http://*:1234/").WithAllCounters();
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

        public abstract string HandlePublishProgress(string request, int processInd);
        public abstract string HandleLog(string request, int processInd);
        protected abstract double GetProgressInPercents();

        public virtual void Dispose()
        {
            metric.Dispose();
        }

        private MetricsConfig metric;
        private int lastReportedToTeamCityProgressPercent;
        protected readonly ITeamCityLogger teamCityLogger;
        protected readonly TestConfiguration configuration;
    }
}