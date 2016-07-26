using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Implementations.RemoteLocks;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.ExternalLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.ExternalLogging.HttpLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.TestConfigurations;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.ProgressMessages;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.Tests
{
    public class TimelineTest : ITest<TimelineProgressMessage>
    {
        public TimelineTest(TestConfiguration configuration, IRemoteLockGetter remoteLockGetter, IExternalProgressLogger<TimelineProgressMessage> externalLogger, HttpExternalDataGetter httpExternalDataGetter, int processInd)
        {
            this.configuration = configuration;
            var lockId = httpExternalDataGetter.GetLockId().Result;
            locker = remoteLockGetter.Get(lockId);
            rand = new Random(Guid.NewGuid().GetHashCode());
            this.externalLogger = externalLogger;
            timeCorrectionDelta = httpExternalDataGetter.GetTime().Result - (long)DateTime.UtcNow.Subtract(new DateTime(1970, 1, 1, 0, 0, 0)).TotalMilliseconds;
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
            var globalTimer = Stopwatch.StartNew();
            for (var i = 0; i < configuration.AmountOfLocksPerThread; i++)
            {
                var lockEvent = new TimelineProgressMessage.LockEvent();
                using (locker.Acquire())
                {
                    lockEvent.AcquiredAt = GetCurrentTimeStamp();
                    var waitTime = rand.Next(configuration.MinWaitTimeMilliseconds, configuration.MaxWaitTimeMilliseconds);
                    Thread.Sleep(waitTime);
                    if (globalTimer.ElapsedMilliseconds > publishIntervalMs)
                    {
                        externalLogger.PublishProgress(new TimelineProgressMessage
                            {
                                LockEvents = lockEvents,
                                Final = false,
                            });
                        lockEvents.Clear();
                        globalTimer.Restart();
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

        private const long publishIntervalMs = 5000;

        private readonly TestConfiguration configuration;
        private readonly IRemoteLock locker;
        private readonly Random rand;
        private readonly IExternalProgressLogger<TimelineProgressMessage> externalLogger;
        private readonly long timeCorrectionDelta;
        private readonly int processInd;
    }
}