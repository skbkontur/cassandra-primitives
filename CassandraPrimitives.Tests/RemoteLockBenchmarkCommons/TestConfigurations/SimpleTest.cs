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
                }
                if (i % logInterval == 0)
                {
                    externalLogger.PublishProgress(new SimpleProgressMessage
                        {
                            LocksAcquired = locksAcquired,
                            AverageLockWaitingTime = stopwatch.ElapsedMilliseconds / locksAcquired,
                            TotalSleepTime = totalSleepTime,
                            TotalTime = globalStopwatch.ElapsedMilliseconds,
                            Final = false
                        });
                    locksAcquired = 0;
                    totalSleepTime = 0;
                    stopwatch.Reset();
                }
            }
            externalLogger.PublishProgress(new SimpleProgressMessage { LocksAcquired = locksAcquired, AverageLockWaitingTime = stopwatch.ElapsedMilliseconds / locksAcquired, Final = false });
        }

        public void TearDown()
        {
            var totalTimeSpent = globalStopwatch.ElapsedMilliseconds;
            externalLogger.PublishProgress(new SimpleProgressMessage {Final = true, TotalTime = totalTimeSpent});
        }
        
        private readonly TestConfiguration configuration;
        private readonly IRemoteLockCreator locker;
        private readonly string lockId;
        private readonly Random rand;
        private Stopwatch globalStopwatch;
        private readonly IExternalProgressLogger<SimpleProgressMessage> externalLogger;
    }
}