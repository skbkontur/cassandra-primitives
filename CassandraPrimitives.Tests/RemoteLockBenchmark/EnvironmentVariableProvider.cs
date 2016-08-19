using System;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.TestConfigurations;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark
{
    public class EnvironmentVariableProvider : IEnvironmentVariableProvider
    {
        public string GetValue(string name)
        {
            return Environment.GetEnvironmentVariable("benchmark." + name);
        }
    }
}