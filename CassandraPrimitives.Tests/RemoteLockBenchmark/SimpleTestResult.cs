namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark
{
    public class SimpleTestResult : ITestResult
    {
        public int LocksCount { get; set; }
        public long TotalWaitTime { get; set; }
        public long TotalTimeSpent { get; set; }

        public SimpleTestResult()
        {
            LocksCount = 0;
            TotalWaitTime = 0;
            TotalTimeSpent = 0;
        }

        public string GetShortMessage()
        {
            return string.Format("{0} ms spent ({1:.00} times slower than unreachable ideal - {2} ms)", TotalTimeSpent, (double)TotalTimeSpent / TotalWaitTime, TotalWaitTime);
        }
    }
}