namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.TestConfigurations
{
    public class TestConfiguration
    {
        public enum RemoteLockImplementation
        {
            Cassandra,
            Zookeeper
        }
        public int amountOfThreads;
        public int amountOfProcesses;
        public int amountOfLocksPerThread;
        public int minWaitTimeMilliseconds;
        public int maxWaitTimeMilliseconds;
        public string remoteHostName;
        public int httpPort;
        public RemoteLockImplementation remoteLockImplementation;
    }
}