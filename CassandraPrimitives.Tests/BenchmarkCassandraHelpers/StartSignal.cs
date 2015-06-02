namespace BenchmarkCassandraHelpers
{
    public class StartSignal
    {
        public string LockId { get; set; }
        public string[] ProcessIds { get; set; }
        public int LocksCount { get; set; }
        public LockType LockType { get; set; }
    }
}