using System;
using System.Net.Http;
using System.Threading.Tasks;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.TestConfigurations;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.ZookeeperSettings;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkChildProcessDriver.ExternalLogging.Http
{
    public class HttpExternalDataGetter : IDisposable
    {
        public HttpExternalDataGetter(string remoteHostName, int port)
        {
            settingsForObjectsWithIpAddresses = new JsonSerializerSettings();
            settingsForObjectsWithIpAddresses.Converters.Add(new IpAddressConverter());
            settingsForObjectsWithIpAddresses.Converters.Add(new IpEndPointConverter());
            settingsForObjectsWithIpAddresses.Formatting = Formatting.Indented;
            this.remoteHostName = remoteHostName;
            httpClient = new HttpClient();
            this.port = port;
        }

        private async Task<string> GetResponse(string method)
        {
            var response = await httpClient.GetAsync(String.Format("http://{0}:{1}/{2}", remoteHostName, port, method));
            var data = await response.Content.ReadAsStringAsync();
            return data;
        }

        private async Task<TResponse> GetAndDecodeResponse<TResponse>(string method, JsonSerializerSettings settings = null)
        {
            var data = await GetResponse(method);
            if (settings == null)
                return JsonConvert.DeserializeObject<TResponse>(data);
            return JsonConvert.DeserializeObject<TResponse>(data, settings);
        }

        private async Task<JObject> GetAndDecodeResponseToJObject(string method)
        {
            var data = await GetResponse(method);
            return JObject.Parse(data);
        }

        public async Task<CassandraClusterSettings> GetCassandraSettings()
        {
            return await GetAndDecodeResponse<CassandraClusterSettings>("get_cassandra_options", settingsForObjectsWithIpAddresses);
        }

        public async Task<ZookeeperClusterSettings> GetZookeeperSettings()
        {
            return await GetAndDecodeResponse<ZookeeperClusterSettings>("get_zookeeper_options", settingsForObjectsWithIpAddresses);
        }

        public async Task<TestConfiguration> GetTestConfiguration()
        {
            return await GetAndDecodeResponse<TestConfiguration>("get_test_configuration");
        }

        public async Task<long> GetTime()
        {
            var responseObject = await GetAndDecodeResponseToJObject("get_time");
            var time = long.Parse(responseObject["UtcMilliseconds"].ToString());
            return time;
        }

        public async Task<string> GetLockId()
        {
            var responseObject = await GetAndDecodeResponseToJObject("get_lock_id");
            var lockId = responseObject["Value"].ToString();
            return lockId;
        }

        public void Dispose()
        {
            httpClient.Dispose();
        }

        private readonly HttpClient httpClient;
        private readonly string remoteHostName;
        private readonly int port;
        private readonly JsonSerializerSettings settingsForObjectsWithIpAddresses;
    }
}