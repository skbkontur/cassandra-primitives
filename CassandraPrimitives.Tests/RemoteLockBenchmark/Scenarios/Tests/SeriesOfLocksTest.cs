using System;
using System.Diagnostics;
using System.Threading;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Implementations.RemoteLocks;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.ExternalLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.ExternalLogging.HttpLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.TestConfigurations;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.ProgressMessages;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.Tests
{
    public class SeriesOfLocksTest : ITest<SeriesOfLocksProgressMessage>
    {
        public SeriesOfLocksTest(TestConfiguration configuration, IRemoteLockGetterProvider remoteLockGetterProvider, IExternalProgressLogger<SeriesOfLocksProgressMessage> externalLogger, HttpExternalDataGetter httpExternalDataGetter)
        {
            this.configuration = configuration;
            lockIdCommonPrefix = httpExternalDataGetter.GetLockId().Result;
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
            for (var i = 0; i < configuration.AmountOfLocksPerThread; i++)
            {
                var locker = remoteLockGetter.Get(lockIdCommonPrefix + string.Format("{0:D20}", i));
                IDisposable remoteLock;
                if (locker.TryAcquire(out remoteLock))
                {
                    using (remoteLock)
                    {
                        amountOfLocks++;
                        var waitTime = rand.Next(configuration.MinWaitTimeMilliseconds, configuration.MaxWaitTimeMilliseconds);
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
        private readonly TestConfiguration configuration;
        private readonly Random rand;
        private readonly IExternalProgressLogger<SeriesOfLocksProgressMessage> externalLogger;
        private readonly IRemoteLockGetterProvider remoteLockGetterProvider;
        private readonly string lockIdCommonPrefix;
    }
}