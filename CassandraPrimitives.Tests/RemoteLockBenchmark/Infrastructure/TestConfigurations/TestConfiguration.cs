using System;
using System.Net.NetworkInformation;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.TestConfigurations
{
    public class TestConfiguration
    {
        public TestConfiguration(int amountOfThreads, int amountOfProcesses, int amountOfLocksPerThread, int minWaitTimeMilliseconds, int maxWaitTimeMilliseconds, int amountOfClusterNodes, string remoteHostName, int httpPort, RemoteLockImplementations remoteLockImplementation)
        {
            AmountOfThreads = amountOfThreads;
            AmountOfProcesses = amountOfProcesses;
            AmountOfLocksPerThread = amountOfLocksPerThread;
            MinWaitTimeMilliseconds = minWaitTimeMilliseconds;
            MaxWaitTimeMilliseconds = maxWaitTimeMilliseconds;
            AmountOfClusterNodes = amountOfClusterNodes;
            RemoteHostName = remoteHostName;
            HttpPort = httpPort;
            RemoteLockImplementation = remoteLockImplementation;
        }

        public static TestConfiguration GetFromEnvironment()
        {
            RemoteLockImplementations remoteLockImplementation;

            var amountOfThreads = GetIntVariableFromEnvironment("benchmark.AmountOfThreads");
            var amountOfProcesses = GetIntVariableFromEnvironment("benchmark.AmountOfProcesses");
            var amountOfLocksPerThread = GetIntVariableFromEnvironment("benchmark.AmountOfLocksPerThread");
            var minWaitTimeMilliseconds = GetIntVariableFromEnvironment("benchmark.MinWaitTimeMilliseconds");
            var maxWaitTimeMilliseconds = GetIntVariableFromEnvironment("benchmark.MaxWaitTimeMilliseconds");
            var amountOfClusterNodes = GetIntVariableFromEnvironment("benchmark.AmountOfClusterNodes");
            var httpPort = GetIntVariableFromEnvironment("benchmark.HttpPort");

            if (!Enum.TryParse(Environment.GetEnvironmentVariable("benchmark.RemoteLockImplementation"), out remoteLockImplementation))
                throw new Exception(string.Format("Invalid value was given for parameter {0}", "benchmark.RemoteLockImplementation"));

            var remoteHostName = IPGlobalProperties.GetIPGlobalProperties().HostName + "." + IPGlobalProperties.GetIPGlobalProperties().DomainName;

            return new TestConfiguration(amountOfThreads, amountOfProcesses, amountOfLocksPerThread, minWaitTimeMilliseconds, maxWaitTimeMilliseconds, amountOfClusterNodes, remoteHostName, httpPort, remoteLockImplementation);
        }

        private static int GetIntVariableFromEnvironment(string name)
        {
            int result;
            if (!int.TryParse(Environment.GetEnvironmentVariable("benchmark.AmountOfThreads"), out result))
                throw new Exception(string.Format("Invalid value was given for parameter {0}", name));
            return result;
        }

        public int AmountOfThreads { get; private set; }
        public int AmountOfProcesses { get; private set; }
        public int AmountOfLocksPerThread { get; private set; }
        public int MinWaitTimeMilliseconds { get; private set; }
        public int MaxWaitTimeMilliseconds { get; private set; }
        public int AmountOfClusterNodes { get; private set; }
        public string RemoteHostName { get; private set; }
        public int HttpPort { get; private set; }
        public RemoteLockImplementations RemoteLockImplementation { get; private set; }
    }
}