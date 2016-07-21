namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.TestConfigurations
{
    public class TestConfiguration
    {
        public int amountOfThreads;
        public int amountOfProcesses;
        public int amountOfLocksPerThread;
        public int minWaitTimeMilliseconds;
        public int maxWaitTimeMilliseconds;
        public string remoteHostName;
    }
}