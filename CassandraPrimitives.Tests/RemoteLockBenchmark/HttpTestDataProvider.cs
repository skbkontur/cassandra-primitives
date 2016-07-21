using System;
using System.IO;

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
            server.AddMethod("get_cassandra_options", async context =>
                {
                    try
                    {
                        using (var stream = new StreamWriter(context.Response.OutputStream))
                        {
                            JsonSerializerSettings settings = new JsonSerializerSettings();
                            settings.Converters.Add(new IpAddressConverter());
                            settings.Converters.Add(new IpEndPointConverter());
                            settings.Formatting = Formatting.Indented;
                            await stream.WriteAsync(JsonConvert.SerializeObject(cassandraClusterSettings, settings));
                        }
                        context.Response.OutputStream.Close();
                    }
                    finally
                    {
                        context.Response.Close();
                    }
                });
            server.AddMethod("get_test_configuration", async context =>
                {
                    try
                    {
                        using (var stream = new StreamWriter(context.Response.OutputStream))
                            await stream.WriteAsync(JsonConvert.SerializeObject(testConfiguration));
                        context.Response.OutputStream.Close();
                    }
                    finally
                    {
                        context.Response.Close();
                    }
                });
            server.AddMethod("get_time", async context =>
                {
                    try
                    {
                        var time = (long)DateTime.UtcNow.Subtract(new DateTime(1970, 1, 1, 0, 0, 0)).TotalMilliseconds;
                        using (var stream = new StreamWriter(context.Response.OutputStream))
                            await stream.WriteAsync(JsonConvert.SerializeObject(new { UtcMilliseconds = time }));
                        context.Response.OutputStream.Close();
                    }
                    finally
                    {
                        context.Response.Close();
                    }
                });
            var lockId = Guid.NewGuid().ToString();
            server.AddMethod("get_lock_id", async context =>
            {
                try
                {
                    using (var stream = new StreamWriter(context.Response.OutputStream))
                        await stream.WriteAsync(JsonConvert.SerializeObject(new { Value = lockId }));
                    context.Response.OutputStream.Close();
                }
                finally
                {
                    context.Response.Close();
                }
            });
        }

        public void Dispose()
        {
            server.Dispose();
        }

        private readonly HttpServer server;
    }
}