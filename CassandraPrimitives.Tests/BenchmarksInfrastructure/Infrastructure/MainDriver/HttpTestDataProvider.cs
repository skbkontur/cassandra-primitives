using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Net;

using Newtonsoft.Json;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.ExternalLogging.HttpLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.TestConfigurations;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.MainDriver
{
    public class HttpTestDataProvider : IDisposable
    {
        public HttpTestDataProvider(TestConfiguration testConfiguration, Dictionary<string, object> optionsSet)
        {
            this.optionsSet = optionsSet;
            server = new HttpServer(testConfiguration.HttpPort);
            settingsForObjectsWithAddresses = new JsonSerializerSettings();
            settingsForObjectsWithAddresses.Converters.Add(new IpAddressConverter());
            settingsForObjectsWithAddresses.Converters.Add(new IpEndPointConverter());
            settingsForObjectsWithAddresses.Formatting = Formatting.Indented;
            server.AddMethod("get_options", c => ProcessRequest(c, ProcessOptionsRequest));
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
                var response = getResponse(context.Request.QueryString);
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

        public void Dispose()
        {
            server.Dispose();
        }

        private readonly HttpServer server;
        private readonly Dictionary<string, object> optionsSet;
        private readonly JsonSerializerSettings settingsForObjectsWithAddresses;
    }
}