using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Implementations.RemoteLocks;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.ExternalLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.ExternalLogging.HttpLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.Tests;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Timeline
{
    public class TimelineTest : ITest<TimelineProgressMessage, TimelineTestOptions>
    {
        public TimelineTest(IRemoteLockGetterProvider remoteLockGetterProvider, IExternalProgressLogger externalLogger, HttpExternalDataGetter httpExternalDataGetter, int processInd)
        {
            testOptions = httpExternalDataGetter.GetTestOptions<TimelineTestOptions>().Result;
            this.remoteLockGetterProvider = remoteLockGetterProvider;
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
            var locker = remoteLockGetterProvider.GetRemoteLockGetter().Get(testOptions.LockId);
            var lockEvents = new List<TimelineProgressMessage.LockEvent>();
            var globalTimer = Stopwatch.StartNew();
            for (var i = 0; i < testOptions.AmountOfLocks; i++)
            {
                var lockEvent = new TimelineProgressMessage.LockEvent();
                using (locker.Acquire())
                {
                    lockEvent.AcquiredAt = GetCurrentTimeStamp();
                    Thread.Sleep(rand.Next(testOptions.MinWaitTimeMilliseconds, testOptions.MaxWaitTimeMilliseconds));
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
                Thread.Sleep(rand.Next(testOptions.MinWaitTimeMilliseconds, testOptions.MaxWaitTimeMilliseconds));
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

        private readonly Random rand;
        private readonly IExternalProgressLogger externalLogger;
        private readonly long timeCorrectionDelta;
        private readonly int processInd;
        private readonly IRemoteLockGetterProvider remoteLockGetterProvider;
        private readonly TimelineTestOptions testOptions;
    }
}