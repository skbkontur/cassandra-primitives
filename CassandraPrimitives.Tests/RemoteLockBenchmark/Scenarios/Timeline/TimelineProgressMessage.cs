using System.Collections.Generic;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.ProgressMessages;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.Timeline
{
    public class TimelineProgressMessage : IProgressMessage
    {
        public List<LockEvent> LockEvents { get; set; }
        public bool Final { get; set; }

        public class LockEvent
        {
            public long AcquiredAt { get; set; }
            public long ReleasedAt { get; set; }
        }

        public class LockEventComparer : IComparer<LockEvent>
        {
            public int Compare(LockEvent x, LockEvent y)
            {
                return (x.ReleasedAt != y.ReleasedAt) ? x.ReleasedAt.CompareTo(y.ReleasedAt) : x.AcquiredAt.CompareTo(y.AcquiredAt);
            }
        }
    }
}