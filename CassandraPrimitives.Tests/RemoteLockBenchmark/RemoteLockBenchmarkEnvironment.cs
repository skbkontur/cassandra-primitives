using System;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.TestConfigurations;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark
{
    public class RemoteLockBenchmarkEnvironment : ITestEnvironment, IRemoteLockBenchmarkEnvironment
    {
        public string AmountOfThreads { get; private set; }
        public string AmountOfProcesses { get; private set; }
        public string AmountOfLocksPerThread { get; private set; }
        public string MinWaitTimeMilliseconds { get; private set; }
        public string MaxWaitTimeMilliseconds { get; private set; }
        public string AmountOfClusterNodes { get; private set; }
        public string RemoteHostName { get; private set; }
        public string HttpPort { get; private set; }
        public string ClusterType { get; private set; }
        public string TestScenario { get; private set; }

        public static RemoteLockBenchmarkEnvironment GetFromEnvironment()
        {
            return new RemoteLockBenchmarkEnvironment
                {
                    AmountOfThreads = Environment.GetEnvironmentVariable("benchmark.AmountOfThreads"),
                    AmountOfProcesses = Environment.GetEnvironmentVariable("benchmark.AmountOfProcesses"),
                    AmountOfLocksPerThread = Environment.GetEnvironmentVariable("benchmark.AmountOfLocksPerThread"),
                    MinWaitTimeMilliseconds = Environment.GetEnvironmentVariable("benchmark.MinWaitTimeMilliseconds"),
                    MaxWaitTimeMilliseconds = Environment.GetEnvironmentVariable("benchmark.MaxWaitTimeMilliseconds"),
                    AmountOfClusterNodes = Environment.GetEnvironmentVariable("benchmark.AmountOfClusterNodes"),
                    RemoteHostName = Environment.GetEnvironmentVariable("benchmark.RemoteHostName"),
                    HttpPort = Environment.GetEnvironmentVariable("benchmark.HttpPort"),
                    ClusterType = Environment.GetEnvironmentVariable("benchmark.ClusterType"),
                    TestScenario = Environment.GetEnvironmentVariable("benchmark.TestScenario"),
                };
        }
    }
}