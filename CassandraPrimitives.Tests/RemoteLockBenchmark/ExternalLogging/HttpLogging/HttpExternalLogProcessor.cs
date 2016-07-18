using System;
using System.IO;
using System.Threading.Tasks;

using Newtonsoft.Json;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.TestConfigurations;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.ExternalLogging.HttpLogging
{
    public class HttpExternalLogProcessor : IExternalLogProcessor<SimpleTestResult>, IDisposable
    {
        public HttpExternalLogProcessor(TestConfiguration configuration, ITeamCityLogger teamCityLogger)
        {
            results = new Task<SimpleTestResult>[configuration.amountOfProcesses];
            var sources = new TaskCompletionSource<SimpleTestResult>[configuration.amountOfProcesses];
            for (int i = 0; i < configuration.amountOfProcesses; i++)
            {
                sources[i] = new TaskCompletionSource<SimpleTestResult>();
                results[i] = sources[i].Task;
            }
            httpServer = new HttpServer();
            httpServer.AddMethod("publish_result", async context =>
                {
                    try
                    {
                        if (context.Request.QueryString["process_ind"] == null)
                            return;

                        var rawData = await new StreamReader(context.Request.InputStream).ReadToEndAsync();
                        var testResult = JsonConvert.DeserializeObject<SimpleTestResult>(rawData);

                        var processInd = int.Parse(context.Request.QueryString["process_ind"]);
                        teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Process {0} finished with result: {1}", processInd, testResult.GetShortMessage());
                        sources[processInd].SetResult(testResult);
                    }
                    finally
                    {
                        context.Response.Close();
                    }
                });
        }

        public void StartProcessingLog()
        {
        }

        public SimpleTestResult GetTestResult(int processInd)
        {
            return results[processInd].Result;
        }

        public void Dispose()
        {
            httpServer.Dispose();
        }

        private readonly HttpServer httpServer;
        private readonly Task<SimpleTestResult>[] results;
    }
}