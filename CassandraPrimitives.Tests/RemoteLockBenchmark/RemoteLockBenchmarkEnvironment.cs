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
        public string HttpPort { get; private set; }
        public string ClusterType { get; private set; }
        public string TestScenario { get; private set; }

        public static RemoteLockBenchmarkEnvironment GetFromEnvironment()
        {
            AssertEnvirinmentVariableDefined("benchmark.AmountOfThreads");
            AssertEnvirinmentVariableDefined("benchmark.AmountOfProcesses");
            AssertEnvirinmentVariableDefined("benchmark.AmountOfLocks");
            AssertEnvirinmentVariableDefined("benchmark.MinWaitTimeMilliseconds");
            AssertEnvirinmentVariableDefined("benchmark.MaxWaitTimeMilliseconds");
            AssertEnvirinmentVariableDefined("benchmark.AmountOfClusterNodes");
            AssertEnvirinmentVariableDefined("benchmark.HttpPort");
            AssertEnvirinmentVariableDefined("benchmark.ClusterType");
            AssertEnvirinmentVariableDefined("benchmark.TestScenario");
            return new RemoteLockBenchmarkEnvironment
                {
                    AmountOfThreads = Environment.GetEnvironmentVariable("benchmark.AmountOfThreads"),
                    AmountOfProcesses = Environment.GetEnvironmentVariable("benchmark.AmountOfProcesses"),
                    AmountOfLocksPerThread = Environment.GetEnvironmentVariable("benchmark.AmountOfLocks"),
                    MinWaitTimeMilliseconds = Environment.GetEnvironmentVariable("benchmark.MinWaitTimeMilliseconds"),
                    MaxWaitTimeMilliseconds = Environment.GetEnvironmentVariable("benchmark.MaxWaitTimeMilliseconds"),
                    AmountOfClusterNodes = Environment.GetEnvironmentVariable("benchmark.AmountOfClusterNodes"),
                    HttpPort = Environment.GetEnvironmentVariable("benchmark.HttpPort"),
                    ClusterType = Environment.GetEnvironmentVariable("benchmark.ClusterType"),
                    TestScenario = Environment.GetEnvironmentVariable("benchmark.TestScenario"),
                };
        }

        private static void AssertEnvirinmentVariableDefined(string name)
        {
            if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable(name)))
                throw new Exception(string.Format("Environment variable {0} was not defined", name));
        }
    }
}