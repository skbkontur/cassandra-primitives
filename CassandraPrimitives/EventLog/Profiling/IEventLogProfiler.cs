using System;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.Profiling
{
    public interface IEventLogProfiler
    {
        void BeforeRake(TimeSpan elapsed, long eventCount, long batchCount);
        void AfterRake(TimeSpan elapsed);
        void AfterWriteBatch(TimeSpan elapsed, int batchLength, int attemptCount);
    }
}