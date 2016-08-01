using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.ProgressMessages;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.SeriesOfLocks
{
    public class SeriesOfLocksProgressMessage : IProgressMessage
    {
        public long AmountOfLocks { get; set; }
        public bool Final { get; set; }
    }
}