using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Implementations.RemoteLocks;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.ExternalLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.ExternalLogging.HttpLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.Tests;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.WaitForLock
{
    public class WaitForLockTest : ITest<WaitForLockProgressMessage, WaitForLockTestOptions>
    {
        public WaitForLockTest(IRemoteLockGetterProvider remoteLockGetterProvider, IExternalProgressLogger externalLogger, HttpExternalDataGetter httpExternalDataGetter)
        {
            testOptions = httpExternalDataGetter.GetTestOptions<WaitForLockTestOptions>().Result;
            this.remoteLockGetterProvider = remoteLockGetterProvider;
            rand = new Random(Guid.NewGuid().GetHashCode());
            this.externalLogger = externalLogger;
        }

        public void SetUp()
        {
        }

        public void DoWorkInSingleThread(int threadInd)
        {
            var locker = remoteLockGetterProvider.GetRemoteLockGetter().Get(testOptions.LockId);
            var lockWaitingDurations = new List<long>();
            var globalTimer = Stopwatch.StartNew();
            for (var i = 0; i < testOptions.AmountOfLocksPerThread; i++)
            {
                var blockedAt = globalTimer.ElapsedMilliseconds;
                using (locker.Acquire())
                {
                    lockWaitingDurations.Add(globalTimer.ElapsedMilliseconds - blockedAt);
                    var waitTime = rand.Next(testOptions.MinWaitTimeMilliseconds, testOptions.MaxWaitTimeMilliseconds);
                    Thread.Sleep(waitTime);
                    if (globalTimer.ElapsedMilliseconds > publishIntervalMs)
                    {
                        externalLogger.PublishProgress(new WaitForLockProgressMessage
                            {
                                LockWaitingDurationsMs = lockWaitingDurations,
                                Final = false,
                            });
                        lockWaitingDurations.Clear();
                        globalTimer.Restart();
                    }
                }
            }
            externalLogger.PublishProgress(new WaitForLockProgressMessage
                {
                    LockWaitingDurationsMs = lockWaitingDurations,
                    Final = false,
                });
        }

        public void TearDown()
        {
            externalLogger.PublishProgress(new WaitForLockProgressMessage {Final = true});
        }

        private const long publishIntervalMs = 5000;

        private readonly Random rand;
        private readonly IExternalProgressLogger externalLogger;
        private readonly IRemoteLockGetterProvider remoteLockGetterProvider;
        private readonly WaitForLockTestOptions testOptions;
    }
}