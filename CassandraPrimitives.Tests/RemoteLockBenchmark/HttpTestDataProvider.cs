using System;
using System.IO;
using System.Net;

using Newtonsoft.Json;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.ExternalLogging.HttpLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.TestConfigurations;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark
{
    public class HttpTestDataProvider : IDisposable
    {
        public HttpTestDataProvider(CassandraClusterSettings cassandraClusterSettings, TestConfiguration testConfiguration)
        {
            server = new HttpServer();
            server.AddMethod("get_cassandra_options", c => ProcessRequest(c, () =>
                {
                    JsonSerializerSettings settings = new JsonSerializerSettings();
                    settings.Converters.Add(new IpAddressConverter());
                    settings.Converters.Add(new IpEndPointConverter());
                    settings.Formatting = Formatting.Indented;
                    return JsonConvert.SerializeObject(cassandraClusterSettings, settings);
                }));
            server.AddMethod("get_test_configuration", c => ProcessRequest(c, () => JsonConvert.SerializeObject(testConfiguration)));
            server.AddMethod("get_time", c => ProcessRequest(c, () =>
                {
                    var time = (long)DateTime.UtcNow.Subtract(new DateTime(1970, 1, 1, 0, 0, 0)).TotalMilliseconds;
                    return JsonConvert.SerializeObject(new {UtcMilliseconds = time});
                }));
            var lockId = Guid.NewGuid().ToString();
            server.AddMethod("get_lock_id", c => ProcessRequest(c, () => JsonConvert.SerializeObject(new {Value = lockId})));
        }

        private async void ProcessRequest(HttpListenerContext context, Func<string> getResponse)
        {
            try
            {
                using (var stream = new StreamWriter(context.Response.OutputStream))
                    await stream.WriteAsync(getResponse());
                context.Response.OutputStream.Close();
            }
            finally
            {
                context.Response.Close();
            }
        }

        public void Dispose()
        {
            server.Dispose();
        }

        private readonly HttpServer server;
    }
}