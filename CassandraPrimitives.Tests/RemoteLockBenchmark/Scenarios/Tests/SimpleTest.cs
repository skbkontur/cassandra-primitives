using System;
using System.Diagnostics;
using System.Threading;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Implementations.RemoteLocks;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.ExternalLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.TestConfigurations;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.ProgressMessages;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.Tests
{
    public class SimpleTest : ITest<SimpleProgressMessage>
    {
        public SimpleTest(TestConfiguration configuration, IRemoteLockGetter remoteLockGetter, IExternalProgressLogger externalLogger)
        {
            this.configuration = configuration;
            lockId = Guid.NewGuid().ToString();
            locker = remoteLockGetter.Get(lockId);
            rand = new Random(Guid.NewGuid().GetHashCode());
            this.externalLogger = externalLogger;
        }

        public void SetUp()
        {
            globalStopwatch = Stopwatch.StartNew();
        }

        public void DoWorkInSingleThread(int threadInd)
        {
            var locksAcquired = 0;
            var totalSleepTime = 0;
            var stopwatch = new Stopwatch();
            var totalStopwatch = Stopwatch.StartNew();
            var logInterval = configuration.AmountOfLocksPerThread / 10;
            for (var i = 0; i < configuration.AmountOfLocksPerThread; i++)
            {
                stopwatch.Start();
                using (locker.Acquire())
                {
                    stopwatch.Stop();
                    locksAcquired++;
                    var waitTime = (int)(rand.NextDouble() * configuration.MaxWaitTimeMilliseconds);
                    totalSleepTime += waitTime;
                    Thread.Sleep(waitTime);
                    if (i % logInterval == 0 || i + 1 == configuration.AmountOfLocksPerThread)
                    {
                        externalLogger.PublishProgress(new SimpleProgressMessage
                            {
                                LocksAcquired = locksAcquired,
                                AverageLockWaitingTime = stopwatch.ElapsedMilliseconds / locksAcquired,
                                SleepTime = totalSleepTime,
                                TotalTime = totalStopwatch.ElapsedMilliseconds,
                                TimeWaitingForLock = stopwatch.ElapsedMilliseconds,
                                Final = false,
                            });
                        locksAcquired = 0;
                        totalSleepTime = 0;
                        stopwatch.Reset();
                        totalStopwatch.Reset();
                        totalStopwatch.Start();
                    }
                }
            }
        }

        public void TearDown()
        {
            var totalTimeSpent = globalStopwatch.ElapsedMilliseconds;
            externalLogger.PublishProgress(new SimpleProgressMessage {Final = true, GlobalTime = totalTimeSpent});
        }

        private readonly TestConfiguration configuration;
        private readonly IRemoteLock locker;
        private readonly string lockId;
        private readonly Random rand;
        private Stopwatch globalStopwatch;
        private readonly IExternalProgressLogger externalLogger;
    }
}