using System;
using System.Collections.Generic;
using System.Linq;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.WaitForLock
{
    public class WaitForLockTestOptions : BaseRemoteLockTestOptions
    {
        public WaitForLockTestOptions(string lockId, int amountOfLocks, int minWaitTimeMilliseconds, int maxWaitTimeMilliseconds)
            : base(amountOfLocks, minWaitTimeMilliseconds, maxWaitTimeMilliseconds)
        {
            LockId = lockId;
        }

        public string LockId { get; private set; }

        public new static List<WaitForLockTestOptions> ParseWithRanges(IRemoteLockBenchmarkEnvironment environment)
        {
            return BaseRemoteLockTestOptions
                .ParseWithRanges(environment)
                .Select(x =>
                        new WaitForLockTestOptions(
                            Guid.NewGuid().ToString(),
                            x.AmountOfLocks,
                            x.MinWaitTimeMilliseconds,
                            x.MaxWaitTimeMilliseconds))
                .ToList();
        }

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