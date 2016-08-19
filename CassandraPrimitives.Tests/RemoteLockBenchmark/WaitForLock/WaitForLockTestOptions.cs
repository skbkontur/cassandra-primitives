namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.WaitForLock
{
    public class WaitForLockTestOptions : BaseRemoteLockTestOptions
    {
        public WaitForLockTestOptions(string lockId, int amountOfLocks, int minWaitTimeMilliseconds, int maxWaitTimeMilliseconds)
            : base(amountOfLocks, minWaitTimeMilliseconds, maxWaitTimeMilliseconds)
        {
            LockId = lockId;
        }

        public WaitForLockTestOptions()
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