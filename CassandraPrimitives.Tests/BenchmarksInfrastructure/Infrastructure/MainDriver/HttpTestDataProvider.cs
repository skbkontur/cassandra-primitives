using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Net;
using System.Threading.Tasks;

using Newtonsoft.Json;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.ExternalLogging.HttpLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.TestConfigurations;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.MainDriver
{
    public class HttpTestDataProvider : IDisposable
    {
        public HttpTestDataProvider(TestConfiguration testConfiguration, Dictionary<string, object> optionsSet, Dictionary<string, Func<object>> dynamicOptionsSet)
        {
            this.optionsSet = optionsSet;
            this.dynamicOptionsSet = dynamicOptionsSet;
            server = new HttpServer(testConfiguration.HttpPort);
            settingsForObjectsWithAddresses = new JsonSerializerSettings();
            settingsForObjectsWithAddresses.Converters.Add(new IpAddressConverter());
            settingsForObjectsWithAddresses.Converters.Add(new IpEndPointConverter());
            settingsForObjectsWithAddresses.Formatting = Formatting.Indented;
            server.AddMethod("get_options", c => ProcessRequest(c, ProcessOptionsRequest));
            server.AddMethod("get_dynamic_option", c => ProcessRequest(c, ProcessDynamicOptionRequest));
            server.AddMethod("get_time", c => ProcessRequest(c, _ =>
                {
                    var time = (long)DateTime.UtcNow.Subtract(new DateTime(1970, 1, 1, 0, 0, 0)).TotalMilliseconds;
                    return JsonConvert.SerializeObject(new {UtcMilliseconds = time});
                }));
        }

        private async void ProcessRequest(HttpListenerContext context, Func<NameValueCollection, string> getResponse)
        {
            try
            {
                var response = await Task.Run(() => getResponse(context.Request.QueryString));
                if (response != null)
                {
                    using (var stream = new StreamWriter(context.Response.OutputStream))
                        await stream.WriteAsync(response);
                }
                else
                    context.Response.OutputStream.Close();
            }
            catch
            {
                context.Response.Close();
            }
        }

        private string ProcessOptionsRequest(NameValueCollection queryString)
        {
            var optionName = queryString["option_name"];
            if (optionName != null && optionsSet.ContainsKey(optionName))
                return JsonConvert.SerializeObject(optionsSet[optionName], settingsForObjectsWithAddresses);
            return null;
        }

        private string ProcessDynamicOptionRequest(NameValueCollection queryString)
        {
            var optionName = queryString["option_name"];
            if (optionName != null && dynamicOptionsSet.ContainsKey(optionName))
                return JsonConvert.SerializeObject(dynamicOptionsSet[optionName](), settingsForObjectsWithAddresses);
            return null;
        }

        public void Dispose()
        {
            server.Dispose();
        }

        private readonly HttpServer server;
        private readonly Dictionary<string, object> optionsSet;
        private readonly JsonSerializerSettings settingsForObjectsWithAddresses;
        private readonly Dictionary<string, Func<object>> dynamicOptionsSet;
    }
}