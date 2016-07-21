using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.ExternalLogging;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.TestConfigurations
{
    public class TimelineTest : ITest
    {
        public TimelineTest(TestConfiguration configuration, IRemoteLockGetter remoteLockGetter, IExternalProgressLogger<TimelineProgressMessage> externalLogger, long timeCorrectionDelta, string lockId, int processInd)
        {
            this.configuration = configuration;
            locker = remoteLockGetter.Get(1).Single();
            this.lockId = lockId;
            rand = new Random(Guid.NewGuid().GetHashCode());
            this.externalLogger = externalLogger;
            this.timeCorrectionDelta = timeCorrectionDelta;
            this.processInd = processInd;
        }

        public void SetUp()
        {
        }

        private long GetCurrentTimeStamp()
        {
            return (long)DateTime.UtcNow.Subtract(new DateTime(1970, 1, 1, 0, 0, 0)).TotalMilliseconds + timeCorrectionDelta;
        }

        public void DoWorkInSingleThread(int threadInd)
        {
            var lockEvents = new List<TimelineProgressMessage.LockEvent>();
            var logInterval = configuration.amountOfLocksPerThread / 10;
            for (var i = 0; i < configuration.amountOfLocksPerThread; i++)
            {
                var lockEvent = new TimelineProgressMessage.LockEvent();
                using (locker.Lock(lockId))
                {
                    lockEvent.AcquiredAt = GetCurrentTimeStamp();
                    var waitTime = rand.Next(configuration.minWaitTimeMilliseconds, configuration.maxWaitTimeMilliseconds);
                    Thread.Sleep(waitTime);
                    if (i % logInterval == 0)
                    {
                        externalLogger.PublishProgress(new TimelineProgressMessage
                            {
                                LockEvents = lockEvents,
                                Final = false,
                            });
                        lockEvents.Clear();
                    }
                    lockEvent.ReleasedAt = GetCurrentTimeStamp();
                }
                lockEvents.Add(lockEvent);
            }
            externalLogger.PublishProgress(new TimelineProgressMessage
                {
                    LockEvents = lockEvents,
                    Final = false,
                });
        }

        public void TearDown()
        {
            externalLogger.PublishProgress(new TimelineProgressMessage {Final = true});
        }

        private readonly TestConfiguration configuration;
        private readonly IRemoteLockCreator locker;
        private readonly string lockId;
        private readonly Random rand;
        private readonly IExternalProgressLogger<TimelineProgressMessage> externalLogger;
        private readonly long timeCorrectionDelta;
        private readonly int processInd;
    }
}