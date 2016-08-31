using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

using log4net;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.ExternalLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.ExternalLogging.HttpLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.Tests;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Implementations;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.SeriesOfLocks
{
    public class SeriesOfLocksTest : ITest<SeriesOfLocksProgressMessage, SeriesOfLocksTestOptions>
    {
        public SeriesOfLocksTest(IRemoteLockGetterProvider remoteLockGetterProvider, IExternalProgressLogger externalLogger, HttpExternalDataGetter httpExternalDataGetter)
        {
            logger = LogManager.GetLogger(GetType());
            this.httpExternalDataGetter = httpExternalDataGetter;
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
            while (!httpExternalDataGetter.GetDynamicOption<bool>("permission_to_start").Result)
                Thread.Sleep(100);
            httpExternalDataGetter.GetDynamicOption<bool>("response_on_start").Wait();
            externalLogger.Log("Thread {0} started", threadInd);

            var remoteLockGetter = remoteLockGetterProvider.GetRemoteLockGetter();
            var locksToRelease = new Queue<Tuple<long, IDisposable>>();
            var globalTimer = Stopwatch.StartNew();
            var reportTimer = Stopwatch.StartNew();
            var amountOfLocks = 0;
            long lastAcquiredLockInd = -1;
            for (long i = 0; i < testOptions.AmountOfLocks; i++)
            {
                try
                {
                    var locker = remoteLockGetter.Get(testOptions.LockIdCommonPrefix + string.Format("{0:D20}", i));
                    IDisposable remoteLock;
                    if (locker.TryAcquire(out remoteLock))
                    {
                        locksToRelease.Enqueue(Tuple.Create(globalTimer.ElapsedMilliseconds, remoteLock));
                        amountOfLocks++;
                        lastAcquiredLockInd = i;
                        logger.InfoFormat("Lock with ind {0} was acquired by thread {1}", i, threadInd);
                        var sleepTime = rand.Next(testOptions.MinWaitTimeMilliseconds, testOptions.MaxWaitTimeMilliseconds);
                        Thread.Sleep(sleepTime);
                        if (reportTimer.ElapsedMilliseconds > publishIntervalMs)
                        {
                            externalLogger.PublishProgress(new SeriesOfLocksProgressMessage
                                {
                                    AmountOfLocks = amountOfLocks,
                                    LastAcquiredLockInd = i,
                                    Final = false,
                                });
                            amountOfLocks = 0;
                            var globalLastLockInd = httpExternalDataGetter.GetDynamicOption<long>("last_acquired_lock_ind").Result;
                            if (globalLastLockInd - i > maxSyncLocksOffset)
                            {
                                externalLogger.Log("Out of sync detected in thread {0} (offset - {1}). Jump to {2}.", threadInd, globalLastLockInd - i, globalLastLockInd);
                                i = globalLastLockInd;
                            }
                            reportTimer.Restart();
                        }
                        while (locksToRelease.Count > 0 && globalTimer.ElapsedMilliseconds - locksToRelease.Peek().Item1 > lockLiveTimeMs)
                            locksToRelease.Dequeue().Item2.Dispose();
                    }
                }
                catch (Exception e)
                {
                    externalLogger.Log("Exception occured in thread {0}:\n{1}", threadInd, e);
                }
            }
            externalLogger.PublishProgress(new SeriesOfLocksProgressMessage
                {
                    AmountOfLocks = amountOfLocks,
                    LastAcquiredLockInd = lastAcquiredLockInd,
                    Final = false,
                });
        }

        public void TearDown()
        {
            externalLogger.PublishProgress(new SeriesOfLocksProgressMessage {Final = true});
        }

        private const long publishIntervalMs = 5000;
        private const long lockLiveTimeMs = 60000;
        private const long maxSyncLocksOffset = 100;
        private readonly Random rand;
        private readonly IExternalProgressLogger externalLogger;
        private readonly IRemoteLockGetterProvider remoteLockGetterProvider;
        private readonly SeriesOfLocksTestOptions testOptions;
        private readonly HttpExternalDataGetter httpExternalDataGetter;
        private readonly ILog logger;
    }
}