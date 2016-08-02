using System;
using System.Collections.Generic;
using System.Linq;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.WaitForLock
{
    public class WaitForLockTestOptions : BaseRemoteLockTestOptions
    {
        public WaitForLockTestOptions(string lockId, int amountOfLocksPerThread, int minWaitTimeMilliseconds, int maxWaitTimeMilliseconds)
            : base(amountOfLocksPerThread, minWaitTimeMilliseconds, maxWaitTimeMilliseconds)
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
                            x.AmountOfLocksPerThread,
                            x.MinWaitTimeMilliseconds,
                            x.MaxWaitTimeMilliseconds))
                .ToList();
        }
    }
}