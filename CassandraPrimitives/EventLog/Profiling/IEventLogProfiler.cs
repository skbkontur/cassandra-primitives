using System;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.Profiling
{
    public interface IEventLogProfiler
    {
        void BeforeRake(TimeSpan elapsed, long eventCount, long batchCount, TimeSpan[] sinceEventsQueuedTimes);

        void AfterRake(
            TimeSpan elapsed,
            TimeSpan getGoodLastEventInfo1Time,
            TimeSpan getGoodLastEventInfo2Time,
            TimeSpan writeEventsTime,
            TimeSpan deleteBadEventsTime,
            TimeSpan setLastEventInfoTime,
            TimeSpan setEventsGoodTime);

        void AfterWriteBatch(TimeSpan elapsed, int batchLength, int attemptCount, TimeSpan timeOfSleep);
        void AfterDeferredResultWaitFinished(TimeSpan timeBetweenSetAndWaitOne);
    }
}