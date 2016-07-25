using System;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web;

using log4net;

using Newtonsoft.Json;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.ExternalLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.ProgressMessages;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkChildProcessDriver.ExternalLogging.Http
{
    public class HttpExternalLogger<TProgressMessage> : IExternalProgressLogger<TProgressMessage>, IDisposable
        where TProgressMessage : IProgressMessage
    {
        public HttpExternalLogger(int processInd, string remoteHostName, string processToken)
        {
            httpClient = new HttpClient();
            this.processInd = processInd;
            this.remoteHostName = remoteHostName;
            this.processToken = processToken;
            logger = LogManager.GetLogger(GetType());
        }

        public void PublishProgress(TProgressMessage progressMessage)
        {
            try
            {
                PublishProgressAsync(progressMessage).Wait();
            }
            catch (Exception e)
            {
                logger.WarnFormat("Exception while publishing progress by http:\n{0}", e);
            }
        }

        public void Log(string message)
        {
            try
            {
                LogAsync(message).Wait();
            }
            catch (Exception e)
            {
                logger.WarnFormat("Exception while logging message by http:\n{0}", e);
            }
        }

        public async Task PublishProgressAsync(TProgressMessage progressMessage)
        {
            var data = JsonConvert.SerializeObject(progressMessage);
            await SendWithProcessIndAndToken("publish_progress", data);
        }

        public async Task LogAsync(string message)
        {
            var objectToSend = new {message = message};
            var data = JsonConvert.SerializeObject(objectToSend);
            await SendWithProcessIndAndToken("log", data);
        }

        private async Task SendWithProcessIndAndToken(string method, string data)
        {
            var builder = new UriBuilder
                {
                    Scheme = Uri.UriSchemeHttp,
                    Port = 12345,
                    Host = remoteHostName,
                    Path = method
                };

            var query = HttpUtility.ParseQueryString(builder.Query);
            query["process_ind"] = processInd.ToString();
            query["process_token"] = processToken;
            builder.Query = query.ToString();
            var stringUri = builder.ToString();

            await httpClient.PostAsync(stringUri, new StringContent(data));
        }

        public void Log(string format, params object[] items)
        {
            Log(String.Format(format, items));
        }

        public void Dispose()
        {
            httpClient.Dispose();
        }

        private readonly HttpClient httpClient;
        private readonly int processInd;
        private readonly string remoteHostName;
        private readonly string processToken;
        private readonly ILog logger;
    }
}