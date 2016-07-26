namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.TestConfigurations
{
    public class TestConfiguration
    {
        public TestConfiguration(int amountOfThreads, int amountOfProcesses, int amountOfLocksPerThread, int minWaitTimeMilliseconds, int maxWaitTimeMilliseconds, string remoteHostName, int httpPort, RemoteLockImplementations remoteLockImplementation)
        {
            AmountOfThreads = amountOfThreads;
            AmountOfProcesses = amountOfProcesses;
            AmountOfLocksPerThread = amountOfLocksPerThread;
            MinWaitTimeMilliseconds = minWaitTimeMilliseconds;
            MaxWaitTimeMilliseconds = maxWaitTimeMilliseconds;
            RemoteHostName = remoteHostName;
            HttpPort = httpPort;
            RemoteLockImplementation = remoteLockImplementation;
        }

        public int AmountOfThreads { get; private set; }
        public int AmountOfProcesses { get; private set; }
        public int AmountOfLocksPerThread { get; private set; }
        public int MinWaitTimeMilliseconds { get; private set; }
        public int MaxWaitTimeMilliseconds { get; private set; }
        public string RemoteHostName { get; private set; }
        public int HttpPort { get; private set; }
        public RemoteLockImplementations RemoteLockImplementation { get; private set; }
    }
}