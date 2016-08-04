using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

using log4net;

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
            for (var i = 0; i < testOptions.AmountOfLocks; i++)
            {
                try
                {
                    var locker = remoteLockGetter.Get(testOptions.LockIdCommonPrefix + string.Format("{0:D20}", i));
                    IDisposable remoteLock;
                    logger.InfoFormat("Thread {0} is going to acqite lock with ind {1}", threadInd, i);
                    if (locker.TryAcquire(out remoteLock))
                    {
                        locksToRelease.Enqueue(Tuple.Create(globalTimer.ElapsedMilliseconds, remoteLock));
                        amountOfLocks++;
                        logger.InfoFormat("Lock with ind {0} was acquired by thread {1}", i, threadInd);
                        var sleepTime = rand.Next(testOptions.MinWaitTimeMilliseconds, testOptions.MaxWaitTimeMilliseconds);
                        logger.InfoFormat("Thread {0} is going to sleep for {1} ms", threadInd, sleepTime);
                        Thread.Sleep(sleepTime);
                        logger.InfoFormat("Thread {0} awake", threadInd);
                        if (reportTimer.ElapsedMilliseconds > publishIntervalMs)
                        {
                            logger.InfoFormat("Thread {0} is going to publish progress", threadInd);
                            externalLogger.PublishProgress(new SeriesOfLocksProgressMessage
                                {
                                    AmountOfLocks = amountOfLocks,
                                    Final = false,
                                });
                            amountOfLocks = 0;
                            reportTimer.Restart();
                            logger.InfoFormat("Thread {0} published progress", threadInd);
                        }
                        logger.InfoFormat("Thread {0} is going to release some locks", threadInd);
                        while (locksToRelease.Count > 0 && globalTimer.ElapsedMilliseconds - locksToRelease.Peek().Item1 > lockLiveTimeMs)
                            locksToRelease.Dequeue().Item2.Dispose();
                        logger.InfoFormat("Thread {0} finished releasing locks", threadInd);
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
                    Final = false,
                });
        }

        public void TearDown()
        {
            externalLogger.PublishProgress(new SeriesOfLocksProgressMessage {Final = true});
        }

        private const long publishIntervalMs = 5000;
        private const long lockLiveTimeMs = 60000;
        private readonly Random rand;
        private readonly IExternalProgressLogger externalLogger;
        private readonly IRemoteLockGetterProvider remoteLockGetterProvider;
        private readonly SeriesOfLocksTestOptions testOptions;
        private readonly HttpExternalDataGetter httpExternalDataGetter;
        private readonly ILog logger;
    }
}