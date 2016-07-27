using System.Collections.Generic;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.ProgressMessages
{
    public class WaitForLockProgressMessage : IProgressMessage
    {
        public List<long> LockWaitingDurationsMs { get; set; }
        public bool Final { get; set; }
    }
}