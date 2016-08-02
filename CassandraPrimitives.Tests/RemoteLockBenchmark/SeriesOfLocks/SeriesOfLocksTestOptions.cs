using System;
using System.Collections.Generic;
using System.Linq;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.SeriesOfLocks
{
    public class SeriesOfLocksTestOptions : BaseRemoteLockTestOptions
    {
        public SeriesOfLocksTestOptions(string lockIdCommonPrefix, int amountOfLocks, int minWaitTimeMilliseconds, int maxWaitTimeMilliseconds)
            : base(amountOfLocks, minWaitTimeMilliseconds, maxWaitTimeMilliseconds)
        {
            LockIdCommonPrefix = lockIdCommonPrefix;
        }

        public string LockIdCommonPrefix { get; private set; }

        public new static List<SeriesOfLocksTestOptions> ParseWithRanges(IRemoteLockBenchmarkEnvironment environment)
        {
            return BaseRemoteLockTestOptions
                .ParseWithRanges(environment)
                .Select(x =>
                        new SeriesOfLocksTestOptions(
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
LockIdCommonPrefix = {3}",
                AmountOfLocks,
                MinWaitTimeMilliseconds,
                MaxWaitTimeMilliseconds,
                LockIdCommonPrefix);
        }
    }
}