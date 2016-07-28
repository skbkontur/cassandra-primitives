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
    public class WaitForLockTest : ITest<WaitForLockProgressMessage>
    {
        public WaitForLockTest(TestConfiguration configuration, IRemoteLockGetterProvider remoteLockGetterProvider, IExternalProgressLogger externalLogger, HttpExternalDataGetter httpExternalDataGetter)
        {
            this.configuration = configuration;
            lockId = httpExternalDataGetter.GetLockId().Result;
            this.remoteLockGetterProvider = remoteLockGetterProvider;
            rand = new Random(Guid.NewGuid().GetHashCode());
            this.externalLogger = externalLogger;
        }

        public void SetUp()
        {
        }

        public void DoWorkInSingleThread(int threadInd)
        {
            var locker = remoteLockGetterProvider.GetRemoteLockGetter().Get(lockId);
            var lockWaitingDurations = new List<long>();
            var globalTimer = Stopwatch.StartNew();
            for (var i = 0; i < configuration.AmountOfLocksPerThread; i++)
            {
                var blockedAt = globalTimer.ElapsedMilliseconds;
                using (locker.Acquire())
                {
                    lockWaitingDurations.Add(globalTimer.ElapsedMilliseconds - blockedAt);
                    var waitTime = rand.Next(configuration.MinWaitTimeMilliseconds, configuration.MaxWaitTimeMilliseconds);
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

        private readonly TestConfiguration configuration;
        private readonly Random rand;
        private readonly IExternalProgressLogger externalLogger;
        private readonly IRemoteLockGetterProvider remoteLockGetterProvider;
        private readonly string lockId;
    }
}