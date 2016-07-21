using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;

using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.ExternalLogging;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.TestConfigurations
{
    public class SimpleTest : ITest
    {
        public SimpleTest(TestConfiguration configuration, IRemoteLockGetter remoteLockGetter, IExternalProgressLogger<SimpleProgressMessage> externalLogger)
        {
            this.configuration = configuration;
            locker = remoteLockGetter.Get(1).Single();
            lockId = Guid.NewGuid().ToString();
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
            var logInterval = configuration.amountOfLocksPerThread / 10;
            for (var i = 0; i < configuration.amountOfLocksPerThread; i++)
            {
                stopwatch.Start();
                using (locker.Lock(lockId))
                {
                    stopwatch.Stop();
                    locksAcquired++;
                    var waitTime = (int)(rand.NextDouble() * configuration.maxWaitTimeMilliseconds);
                    totalSleepTime += waitTime;
                    Thread.Sleep(waitTime);
                    if (i % logInterval == 0 || i + 1 == configuration.amountOfLocksPerThread)
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
        private readonly IRemoteLockCreator locker;
        private readonly string lockId;
        private readonly Random rand;
        private Stopwatch globalStopwatch;
        private readonly IExternalProgressLogger<SimpleProgressMessage> externalLogger;
    }
}