namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.ProgressMessages
{
    public class SeriesOfLocksProgressMessage : IProgressMessage
    {
        public long AmountOfLocks { get; set; }
        public bool Final { get; set; }
    }
}