using System.Collections.Generic;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.TestConfigurations
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
    }
}