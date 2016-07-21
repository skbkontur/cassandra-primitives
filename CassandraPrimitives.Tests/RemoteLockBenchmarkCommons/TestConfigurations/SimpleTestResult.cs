using System;
using System.Collections.Generic;
using System.Linq;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.TestConfigurations
{
    public class SimpleTestResult
    {
        public int LocksCount { get; set; }
        public long TotalSleepTime { get; set; }
        public long TotalTimeSpent { get; set; }

        public static Merged Merge(IEnumerable<SimpleTestResult> results)
        {
            return results.Aggregate(new Merged(), (merge, next) => new Merged
                {
                    LocksCount = merge.LocksCount + next.LocksCount,
                    TotalTimeSpent = merge.TotalTimeSpent + next.TotalTimeSpent,
                    TotalWaitTime = merge.TotalWaitTime + next.TotalSleepTime
                });
        }

        public string GetShortMessage()
        {
            return string.Format("{0} ms spent ({1:.00} times slower than unreachable ideal - {2} ms)", TotalTimeSpent, (double)TotalTimeSpent / TotalSleepTime, TotalSleepTime);
        }

        public class Merged
        {
            public int LocksCount { get; set; }
            public long TotalWaitTime { get; set; }
            public long TotalTimeSpent { get; set; }

            public string GetShortMessage()
            {
                return string.Format("{0} ms spent in total ({1:.00} times slower than unreachable ideal - {2} ms)", TotalTimeSpent, (double)TotalTimeSpent / TotalWaitTime, TotalWaitTime);
            }
        }
    }
}