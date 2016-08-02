using System;
using System.Collections.Generic;
using System.Linq;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.SeriesOfLocks
{
    public class SeriesOfLocksTestOptions : BaseRemoteLockTestOptions
    {
        public SeriesOfLocksTestOptions(string lockIdCommonPrefix, int amountOfLocksPerThread, int minWaitTimeMilliseconds, int maxWaitTimeMilliseconds)
            : base(amountOfLocksPerThread, minWaitTimeMilliseconds, maxWaitTimeMilliseconds)
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
                            x.AmountOfLocksPerThread,
                            x.MinWaitTimeMilliseconds,
                            x.MaxWaitTimeMilliseconds))
                .ToList();
        }
    }
}