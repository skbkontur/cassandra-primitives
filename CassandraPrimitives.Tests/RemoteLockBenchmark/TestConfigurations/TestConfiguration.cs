namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.TestConfigurations
{
    public class TestConfiguration
    {
        public int amountOfThreads;
        public int amountOfProcesses;
        public int amountOfLocksPerThread;
        public int maxWaitTimeMilliseconds;
        public string remoteHostName;
    }
}