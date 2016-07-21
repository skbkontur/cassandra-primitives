using System.Collections.Generic;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.TestConfigurations
{
    public class TimelineProgressMessage : IProgressMessage
    {
        public class LockEvent
        {
            public long AcquiredAt { get; set; }
            public long ReleasedAt { get; set; }
        }
        public List<LockEvent> LockEvents { get; set; }
        public bool Final { get; set; }
    }
}