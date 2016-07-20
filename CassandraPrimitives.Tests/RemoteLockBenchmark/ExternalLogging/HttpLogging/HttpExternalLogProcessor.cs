using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

using Metrics;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Agents;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.TestConfigurations;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.ExternalLogging.HttpLogging
{
    public class HttpExternalLogProcessor : IExternalLogProcessor<SimpleTestResult>, IDisposable
    {
        public HttpExternalLogProcessor(TestConfiguration configuration, ITeamCityLogger teamCityLogger, List<RemoteAgentInfo> agents)
        {
            this.configuration = configuration;
            this.agents = agents;
            results = new SimpleTestResult[configuration.amountOfProcesses];
            this.teamCityLogger = teamCityLogger;
            sourcesForWaitingProcesses = new TaskCompletionSource<SimpleTestResult>[configuration.amountOfProcesses];
            for (int i = 0; i < configuration.amountOfProcesses; i++)
            {
                sourcesForWaitingProcesses[i] = new TaskCompletionSource<SimpleTestResult>();
                results[i] = new SimpleTestResult();
            }
            httpServer = new HttpServer();
            httpServer.AddMethod("publish_progress", c => HandleRequestWithProcessInd(c, HandlePublishProgress));
            httpServer.AddMethod("log", c => HandleRequestWithProcessInd(c, HandleLog));
            
            metric = Metric.Config.WithHttpEndpoint("http://*:1234/").WithAllCounters();
            meter = Metric.Meter("Total locks made", new Unit("Locks"));
            histogram = Metric.Histogram("Lock waiting time", new Unit("Milliseconds"));
        }

        private async void HandleRequestWithProcessInd(HttpListenerContext context, Func<HttpListenerContext, int, Task> contextHandler)
        {
            try
            {
                int processInd;
                if (!int.TryParse(context.Request.QueryString["process_ind"], out processInd) || processInd < 0 || processInd >= configuration.amountOfProcesses)
                    return;
                if (!agents.Any(x => x.Token.Equals(context.Request.QueryString["process_token"])))
                    return;
                await contextHandler(context, processInd);
            }
            finally
            {
                context.Response.Close();
            }
        }

        private async Task HandlePublishProgress(HttpListenerContext context, int processInd)
        {
            var rawData = await new StreamReader(context.Request.InputStream).ReadToEndAsync();
            var progressMessage = JsonConvert.DeserializeObject<SimpleProgressMessage>(rawData);

            meter.Mark(progressMessage.LocksAcquired);
            histogram.Update(progressMessage.AverageLockWaitingTime);
            results[processInd].LocksCount += progressMessage.LocksAcquired;
            results[processInd].TotalWaitTime += progressMessage.TotalSleepTime;
            if (progressMessage.Final)
            {
                results[processInd].TotalTimeSpent = progressMessage.TotalTime;
                teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Process {0} finished with result: {1}", processInd, results[processInd].GetShortMessage());
                sourcesForWaitingProcesses[processInd].SetResult(results[processInd]);
            }
            else
            {
                teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Process {0} published intermediate result: Average lock waiting time {1}", processInd, progressMessage.AverageLockWaitingTime);
            }
        }

        private async Task HandleLog(HttpListenerContext context, int processInd)
        {
            var rawData = await new StreamReader(context.Request.InputStream).ReadToEndAsync();
            var log = JObject.Parse(rawData);
            var message = log["message"].ToString();

            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Process {0} says: {1}", processInd, message);
        }

        public void StartProcessingLog()
        {
        }

        public SimpleTestResult GetTestResult(int processInd)
        {
            return sourcesForWaitingProcesses[processInd].Task.Result;
        }

        public void Dispose()
        {
            httpServer.Dispose();
            metric.Dispose();//TODO: Do we need it?
        }

        private readonly HttpServer httpServer;
        private readonly SimpleTestResult[] results;
        private readonly TaskCompletionSource<SimpleTestResult>[] sourcesForWaitingProcesses;
        private readonly ITeamCityLogger teamCityLogger;
        private readonly MetricsConfig metric;
        private readonly Meter meter;
        private readonly Histogram histogram;
        private readonly TestConfiguration configuration;
        private readonly List<RemoteAgentInfo> agents;
    }
}