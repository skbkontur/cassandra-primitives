using System;

namespace SkbKontur.Cassandra.Primitives.EventLog.Profiling
{
    public sealed class EventLogNullProfiler : IEventLogProfiler
    {
        public void BeforeRake(TimeSpan elapsed, long eventCount, long batchCount, TimeSpan[] sinceEventsQueuedTimes)
        {
        }

        public void AfterRake(
            TimeSpan elapsed,
            TimeSpan getGoodLastEventInfo1Time,
            TimeSpan getGoodLastEventInfo2Time,
            TimeSpan writeEventsTime,
            TimeSpan deleteBadEventsTime,
            TimeSpan setLastEventInfoTime,
            TimeSpan setEventsGoodTime)
        {
        }

        public void AfterWriteBatch(TimeSpan elapsed, int batchLength, int attemptCount, TimeSpan timeOfSleep)
        {
        }

        public void AfterDeferredResultWaitFinished(TimeSpan timeBetweenSetAndWaitOne)
        {
        }
    }
}