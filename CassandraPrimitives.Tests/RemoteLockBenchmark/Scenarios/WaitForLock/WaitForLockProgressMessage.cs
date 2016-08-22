using System.Collections.Generic;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.ProgressMessages;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.WaitForLock
{
    public class WaitForLockProgressMessage : IProgressMessage
    {
        public List<long> LockWaitingDurationsMs { get; set; }
        public bool Final { get; set; }
    }
}