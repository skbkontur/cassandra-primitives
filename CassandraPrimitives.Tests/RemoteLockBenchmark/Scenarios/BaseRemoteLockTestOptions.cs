using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.BenchmarkConfiguration.TestOptions;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.TestOptions;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios
{
    public class BaseRemoteLockTestOptions : ITestOptions
    {
        public BaseRemoteLockTestOptions(int amountOfLocks, int minWaitTimeMilliseconds, int maxWaitTimeMilliseconds)
        {
            AmountOfLocks = amountOfLocks;
            MinWaitTimeMilliseconds = minWaitTimeMilliseconds;
            MaxWaitTimeMilliseconds = maxWaitTimeMilliseconds;
        }

        protected BaseRemoteLockTestOptions()
        {
        }

        [TestOption]
        public int AmountOfLocks { get; set; }

        [TestOption]
        public int MinWaitTimeMilliseconds { get; set; }

        [TestOption]
        public int MaxWaitTimeMilliseconds { get; set; }
    }
}