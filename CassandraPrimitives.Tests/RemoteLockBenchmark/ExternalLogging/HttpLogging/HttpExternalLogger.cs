using System;
using System.Net.Http;
using System.Web;

using Newtonsoft.Json;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.TestConfigurations;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.ExternalLogging.HttpLogging
{
    public class HttpExternalLogger : IExternalProgressLogger<SimpleTestResult>, IDisposable
    {
        public HttpExternalLogger(int processInd)
        {
            httpClient = new HttpClient();
            this.processInd = processInd;
        }

        public async void PublishResult(SimpleTestResult testResult)
        {
            var data = JsonConvert.SerializeObject(testResult);

            var builder = new UriBuilder
                {
                    Scheme = Uri.UriSchemeHttp,
                    Port = 12345,
                    Host = "K1606012.kontur",
                    Path = "publish_result"
                };

            var query = HttpUtility.ParseQueryString(builder.Query);
            query["process_ind"] = processInd.ToString();
            builder.Query = query.ToString();
            var stringUri = builder.ToString();

            await httpClient.PostAsync(stringUri, new StringContent(data));
        }

        public async void Log(string message)
        {
            await httpClient.PostAsync("http://K1606012.kontur:12345/log", new StringContent(message));
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
    }
}