using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.ProgressMessages;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.SeriesOfLocks
{
    public class SeriesOfLocksProgressMessage : IProgressMessage
    {
        public long AmountOfLocks { get; set; }
        public bool Final { get; set; }
        public long LastAcquiredLockInd { get; set; }
    }
}