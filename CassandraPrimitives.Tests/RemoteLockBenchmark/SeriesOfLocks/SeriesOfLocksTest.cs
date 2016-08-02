using System;
using System.Diagnostics;
using System.Threading;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Implementations.RemoteLocks;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.ExternalLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.ExternalLogging.HttpLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.Tests;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.SeriesOfLocks
{
    public class SeriesOfLocksTest : ITest<SeriesOfLocksProgressMessage, SeriesOfLocksTestOptions>
    {
        public SeriesOfLocksTest(IRemoteLockGetterProvider remoteLockGetterProvider, IExternalProgressLogger externalLogger, HttpExternalDataGetter httpExternalDataGetter)
        {
            testOptions = httpExternalDataGetter.GetTestOptions<SeriesOfLocksTestOptions>().Result;
            this.remoteLockGetterProvider = remoteLockGetterProvider;
            rand = new Random(Guid.NewGuid().GetHashCode());
            this.externalLogger = externalLogger;
        }

        public void SetUp()
        {
        }

        public void DoWorkInSingleThread(int threadInd)
        {
            var remoteLockGetter = remoteLockGetterProvider.GetRemoteLockGetter();
            var globalTimer = Stopwatch.StartNew();
            var amountOfLocks = 0;
            for (var i = 0; i < testOptions.AmountOfLocksPerThread; i++)
            {
                var locker = remoteLockGetter.Get(testOptions.LockIdCommonPrefix + string.Format("{0:D20}", i));
                IDisposable remoteLock;
                if (locker.TryAcquire(out remoteLock))
                {
                    using (remoteLock)
                    {
                        amountOfLocks++;
                        var waitTime = rand.Next(testOptions.MinWaitTimeMilliseconds, testOptions.MaxWaitTimeMilliseconds);
                        Thread.Sleep(waitTime);
                        if (globalTimer.ElapsedMilliseconds > publishIntervalMs)
                        {
                            externalLogger.PublishProgress(new SeriesOfLocksProgressMessage
                                {
                                    AmountOfLocks = amountOfLocks,
                                    Final = false,
                                });
                            amountOfLocks = 0;
                            globalTimer.Restart();
                        }
                    }
                }
            }
            externalLogger.PublishProgress(new SeriesOfLocksProgressMessage
                {
                    AmountOfLocks = amountOfLocks,
                    Final = false,
                });
        }

        public void TearDown()
        {
            externalLogger.PublishProgress(new SeriesOfLocksProgressMessage {Final = true});
        }

        private const long publishIntervalMs = 5000;
        private readonly Random rand;
        private readonly IExternalProgressLogger externalLogger;
        private readonly IRemoteLockGetterProvider remoteLockGetterProvider;
        private readonly SeriesOfLocksTestOptions testOptions;
    }
}