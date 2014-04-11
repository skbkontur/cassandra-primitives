using System;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.Profiling
{
    internal class EventLogNullProfiler : IEventLogProfiler
    {
        public void BeforeRake(TimeSpan elapsed, long eventCount, long batchCount)
        {
        }

        public void AfterRake(TimeSpan elapsed)
        {
        }

        public void AfterWriteBatch(TimeSpan elapsed, int batchLength, int attemptCount)
        {
        }
    }
}