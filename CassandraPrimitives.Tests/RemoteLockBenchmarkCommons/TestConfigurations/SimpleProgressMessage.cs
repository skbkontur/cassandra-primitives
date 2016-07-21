namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.TestConfigurations
{
    public class SimpleProgressMessage : IProgressMessage
    {
        public int LocksAcquired { get; set; }
        public long AverageLockWaitingTime { get; set; }
        public bool Final { get; set; }
        public long TotalTime { get; set; }
        public long SleepTime { get; set; }
        public long TimeWaitingForLock { get; set; }
        public long GlobalTime { get; set; }
    }
}