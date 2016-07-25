using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Agents;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.ExternalLogging.TestProgressProcessors;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.TestConfigurations;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.ExternalLogging.HttpLogging
{
    public class HttpExternalLogProcessor : IExternalLogProcessor, IDisposable
    {
        public HttpExternalLogProcessor(TestConfiguration configuration, ITeamCityLogger teamCityLogger, List<RemoteAgentInfo> agents, ITestProgressProcessor testProgressProcessor)
        {
            this.configuration = configuration;
            this.agents = agents;
            this.teamCityLogger = teamCityLogger;
            this.testProgressProcessor = testProgressProcessor;

            httpServer = new HttpServer(configuration.httpPort);
            httpServer.AddMethod("publish_progress", c => HandleRequestWithProcessInd(c, testProgressProcessor.HandlePublishProgress));
            httpServer.AddMethod("log", c => HandleRequestWithProcessInd(c, testProgressProcessor.HandleLog));
        }

        private async void HandleRequestWithProcessInd(HttpListenerContext context, Func<string, int, string> contextHandler)
        {
            try
            {
                int processInd;
                if (!int.TryParse(context.Request.QueryString["process_ind"], out processInd) || processInd < 0 || processInd >= configuration.amountOfProcesses)
                    return;
                if (!agents.Any(x => x.Token.Equals(context.Request.QueryString["process_token"])))
                    return;
                string request;
                using (var stream = new StreamReader(context.Request.InputStream))
                    request = await stream.ReadToEndAsync();
                var response = contextHandler(request, processInd);
                if (!String.IsNullOrEmpty(response))
                {
                    using (var stream = new StreamWriter(context.Response.OutputStream))
                        await stream.WriteAsync(response);
                }
                else
                    context.Response.OutputStream.Close();
            }
            catch (HttpListenerException e)
            {
                Console.WriteLine("Network error while processing request:\n{0}", e);
            }
            catch (Exception e)
            {
                Console.WriteLine("Unexpected error while processing request:\n{0}", e);
            }
        }

        public void StartProcessingLog()
        {
        }

        public void Dispose()
        {
            httpServer.Dispose();
        }

        private readonly HttpServer httpServer;
        private readonly ITeamCityLogger teamCityLogger;
        private readonly TestConfiguration configuration;
        private readonly List<RemoteAgentInfo> agents;
        private readonly ITestProgressProcessor testProgressProcessor;
    }
}