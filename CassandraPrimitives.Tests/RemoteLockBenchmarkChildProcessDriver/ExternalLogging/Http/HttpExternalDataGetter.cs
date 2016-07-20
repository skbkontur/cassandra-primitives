using System;
using System.Net.Http;
using System.Threading.Tasks;

using Newtonsoft.Json;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.TestConfigurations;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkChildProcessDriver.ExternalLogging.Http
{
    public class HttpExternalDataGetter : IDisposable
    {
        public HttpExternalDataGetter(string remoteHostName)
        {
            this.remoteHostName = remoteHostName;
            httpClient = new HttpClient();
        }

        public async Task<CassandraClusterSettings> GetCassandraSettings()
        {
            var response = await httpClient.GetAsync(String.Format("http://{0}:12345/get_cassandra_options", remoteHostName));
            var data = await response.Content.ReadAsStringAsync();
            JsonSerializerSettings settings = new JsonSerializerSettings();
            settings.Converters.Add(new IpAddressConverter());
            settings.Converters.Add(new IpEndPointConverter());
            settings.Formatting = Formatting.Indented;
            return JsonConvert.DeserializeObject<CassandraClusterSettings>(data, settings);
        }

        public async Task<TestConfiguration> GetTestConfiguration()
        {
            var response = await httpClient.GetAsync(String.Format("http://{0}:12345/get_test_configuration", remoteHostName));
            var data = await response.Content.ReadAsStringAsync();
            return JsonConvert.DeserializeObject<TestConfiguration>(data);
        }

        public void Dispose()
        {
            httpClient.Dispose();
        }

        private readonly HttpClient httpClient;
        private readonly string remoteHostName;
    }
}