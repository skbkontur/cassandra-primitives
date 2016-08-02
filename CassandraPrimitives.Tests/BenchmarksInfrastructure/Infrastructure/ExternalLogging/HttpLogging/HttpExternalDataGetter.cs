using System;
using System.Collections.Specialized;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Implementations.ZooKeeper.ZookeeperSettings;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.TestConfigurations;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.ExternalLogging.HttpLogging
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

        private async Task<string> GetResponse(string method, NameValueCollection parameters)
        {
            var builder = new UriBuilder
                {
                    Scheme = Uri.UriSchemeHttp,
                    Port = port,
                    Host = remoteHostName,
                    Path = method
                };

            var query = HttpUtility.ParseQueryString(builder.Query);
            query.Add(parameters);
            builder.Query = query.ToString();
            var stringUri = builder.ToString();

            var response = await httpClient.GetAsync(stringUri);
            var data = await response.Content.ReadAsStringAsync();
            return data;
        }

        private async Task<TResponse> GetAndDecodeResponse<TResponse>(string method, NameValueCollection parameters, JsonSerializerSettings settings = null)
        {
            var data = await GetResponse(method, parameters);
            if (settings == null)
                return JsonConvert.DeserializeObject<TResponse>(data);
            return JsonConvert.DeserializeObject<TResponse>(data, settings);
        }

        private async Task<JObject> GetAndDecodeResponseToJObject(string method, NameValueCollection parameters)
        {
            var data = await GetResponse(method, parameters);
            return JObject.Parse(data);
        }

        public async Task<CassandraClusterSettings> GetCassandraSettings()
        {
            return await GetOption<CassandraClusterSettings>("CassandraClusterSettings");
        }

        public async Task<ZookeeperClusterSettings> GetZookeeperSettings()
        {
            return await GetOption<ZookeeperClusterSettings>("ZookeeperClusterSettings");
        }

        public async Task<TestConfiguration> GetTestConfiguration()
        {
            return await GetOption<TestConfiguration>("TestConfiguration");
        }

        public async Task<TTestOptions> GetTestOptions<TTestOptions>()
        {
            return await GetOption<TTestOptions>("TestOptions");
        }

        public async Task<TOption> GetOption<TOption>(string name)
        {
            return await GetAndDecodeResponse<TOption>("get_options", new NameValueCollection {{"option_name", name}}, settingsForObjectsWithIpAddresses);
        }

        public async Task<TOption> GetDynamicOption<TOption>(string name)
        {
            return await GetAndDecodeResponse<TOption>("get_dynamic_option", new NameValueCollection {{"option_name", name}}, settingsForObjectsWithIpAddresses);
        }

        public async Task<long> GetTime()
        {
            var responseObject = await GetAndDecodeResponseToJObject("get_time", new NameValueCollection());
            var time = long.Parse(responseObject["UtcMilliseconds"].ToString());
            return time;
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