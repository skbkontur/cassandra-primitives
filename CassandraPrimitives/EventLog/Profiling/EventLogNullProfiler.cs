using System;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.Profiling
{
    internal sealed class EventLogNullProfiler : IEventLogProfiler
    {
        private EventLogNullProfiler()
        {
        }

        public void BeforeRake(TimeSpan elapsed, long eventCount, long batchCount, TimeSpan[] sinceEventsQueuedTimes)
        {
        }

        public void AfterRake(
            TimeSpan elapsed,
            TimeSpan getGoodLastEventInfo1Time,
            TimeSpan getGoodLastEventInfo2Time,
            TimeSpan writeEventsTime,
            TimeSpan deleteBadEventsTime,
            TimeSpan setEventMetasTime,
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

        public static EventLogNullProfiler Instance
        {
            get
            {
                if(instance == null)
                {
                    lock(instanceLockObject)
                    {
                        if(instance == null)
                            instance = new EventLogNullProfiler();
                    }
                }
                return instance;
            }
        }

        private static volatile EventLogNullProfiler instance;
        private static readonly object instanceLockObject = new object();
    }
}