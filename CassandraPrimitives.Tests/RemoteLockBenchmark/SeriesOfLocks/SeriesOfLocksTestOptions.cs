namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.SeriesOfLocks
{
    public class SeriesOfLocksTestOptions : BaseRemoteLockTestOptions
    {
        public SeriesOfLocksTestOptions(string lockIdCommonPrefix, int amountOfLocks, int minWaitTimeMilliseconds, int maxWaitTimeMilliseconds)
            : base(amountOfLocks, minWaitTimeMilliseconds, maxWaitTimeMilliseconds)
        {
            LockIdCommonPrefix = lockIdCommonPrefix;
        }

        public SeriesOfLocksTestOptions()
        {
        }

        public string LockIdCommonPrefix { get; set; }

        public override string ToString()
        {
            return string.Format(
                @"AmountOfLocks = {0}
MinWaitTimeMilliseconds = {1}
MaxWaitTimeMilliseconds = {2}
LockIdCommonPrefix = {3}",
                AmountOfLocks,
                MinWaitTimeMilliseconds,
                MaxWaitTimeMilliseconds,
                LockIdCommonPrefix);
        }
    }
}