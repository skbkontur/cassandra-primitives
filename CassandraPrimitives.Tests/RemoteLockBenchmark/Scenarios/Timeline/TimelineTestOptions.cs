namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.Timeline
{
    public class TimelineTestOptions : BaseRemoteLockTestOptions
    {
        public TimelineTestOptions(string lockId, int amountOfLocks, int minWaitTimeMilliseconds, int maxWaitTimeMilliseconds)
            : base(amountOfLocks, minWaitTimeMilliseconds, maxWaitTimeMilliseconds)
        {
            LockId = lockId;
        }

        public TimelineTestOptions()
        {
        }

        public string LockId { get; set; }

        public override string ToString()
        {
            return string.Format(
                @"AmountOfLocks = {0}
MinWaitTimeMilliseconds = {1}
MaxWaitTimeMilliseconds = {2}
LockId = {3}",
                AmountOfLocks,
                MinWaitTimeMilliseconds,
                MaxWaitTimeMilliseconds,
                LockId);
        }
    }
}