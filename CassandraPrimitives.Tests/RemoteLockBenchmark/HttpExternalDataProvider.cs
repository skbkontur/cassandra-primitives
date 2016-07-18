using System;
using System.Net.Http;
using System.Threading.Tasks;

using Newtonsoft.Json;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark
{
    public class HttpExternalDataProvider : IDisposable
    {
        public HttpExternalDataProvider()
        {
            httpClient = new HttpClient();
        }

        public async Task<CassandraClusterSettings> GetCassandraSettings()
        {
            var response = await httpClient.GetAsync("http://K1606012.kontur:12345/get_cassandra_options");
            var data = await response.Content.ReadAsStringAsync();
            JsonSerializerSettings settings = new JsonSerializerSettings();
            settings.Converters.Add(new IpAddressConverter());
            settings.Converters.Add(new IpEndPointConverter());
            settings.Formatting = Formatting.Indented;
            return JsonConvert.DeserializeObject<CassandraClusterSettings>(data, settings);
        }

        public void Dispose()
        {
            httpClient.Dispose();
        }

        private readonly HttpClient httpClient;
    }
}