using System;
using System.Threading.Tasks;

using Metrics;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.TestConfigurations;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.ExternalLogging.TestProcessors
{
    public class SimpleTestProgressProcessor : ITestProgressProcessor, IDisposable
    {
        public SimpleTestProgressProcessor(TestConfiguration configuration, ITeamCityLogger teamCityLogger)
        {
            this.teamCityLogger = teamCityLogger;

            results = new SimpleTestResult[configuration.amountOfProcesses];
            sourcesForWaitingProcesses = new TaskCompletionSource<SimpleTestResult>[configuration.amountOfProcesses];
            for (int i = 0; i < configuration.amountOfProcesses; i++)
            {
                sourcesForWaitingProcesses[i] = new TaskCompletionSource<SimpleTestResult>();
                results[i] = new SimpleTestResult();
            }

            metric = Metric.Config.WithHttpEndpoint("http://*:1234/").WithAllCounters();
            meter = Metric.Meter("Total locks made", new Unit("Locks"));
            histogram = Metric.Histogram("Lock waiting time", new Unit("Milliseconds"));
        }

        public string HandlePublishProgress(string request, int processInd)
        {
            var progressMessage = JsonConvert.DeserializeObject<SimpleProgressMessage>(request);

            meter.Mark(progressMessage.LocksAcquired);
            histogram.Update(progressMessage.AverageLockWaitingTime);
            results[processInd].LocksCount += progressMessage.LocksAcquired;
            results[processInd].TotalSleepTime += progressMessage.SleepTime;
            if (progressMessage.Final)
            {
                results[processInd].TotalTimeSpent = progressMessage.GlobalTime;
                teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Process {0} finished with result: {1}", processInd, results[processInd].GetShortMessage());
                sourcesForWaitingProcesses[processInd].SetResult(results[processInd]);
            }
            else
                teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Process {0} published intermediate result: Average lock waiting time {1}", processInd, progressMessage.AverageLockWaitingTime);
            return null;
        }

        public string HandleLog(string request, int processInd)
        {
            var log = JObject.Parse(request);
            var message = log["message"].ToString();

            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Process {0} says: {1}", processInd, message);
            return null;
        }

        public SimpleTestResult GetTestResult(int processInd)
        {
            return sourcesForWaitingProcesses[processInd].Task.Result;
        }

        public void Dispose()
        {
            metric.Dispose();
        }

        private readonly MetricsConfig metric;
        private readonly Meter meter;
        private readonly Histogram histogram;
        private readonly ITeamCityLogger teamCityLogger;
        private readonly SimpleTestResult[] results;
        private readonly TaskCompletionSource<SimpleTestResult>[] sourcesForWaitingProcesses;
    }
}