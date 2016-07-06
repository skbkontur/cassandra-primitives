using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;

using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark
{
    class SimpleTest : ITest<SimpleTestResult>
    {
        private readonly TestConfiguration configuration;
        private readonly IRemoteLockCreator locker;
        private readonly string lockId;
        private readonly Random rand;
        private readonly SimpleTestResult testResult;

        public SimpleTest(TestConfiguration configuration, IRemoteLockGetter remoteLockGetter)
        {
            this.configuration = configuration;
            locker = remoteLockGetter.Get(1).Single();
            lockId = Guid.NewGuid().ToString();
            rand = new Random(Guid.NewGuid().GetHashCode());
            testResult = new SimpleTestResult();
        }

        public void Run()
        {
            var threads = new Thread[configuration.amountOfThreads];
            for (int i = 0; i < configuration.amountOfThreads; i++)
            {
                var threadInd = i;
                threads[i] = new Thread(() => DoWorkInSingleThread(threadInd));
            }

            var stopwatch = Stopwatch.StartNew();

            foreach (var thread in threads)
                thread.Start();
            foreach (var thread in threads)
                thread.Join();//TODO timeout?

            testResult.TotalTimeSpent = stopwatch.ElapsedMilliseconds;
        }

        private void DoWorkInSingleThread(int threadId)
        {
            for (int i = 0; i < configuration.amountOfLocksPerThread; i++)
            {
                using (locker.Lock(lockId))
                {
                    testResult.LocksCount++;
                    var waitTime = (int)(rand.NextDouble() * configuration.maxWaitTimeMilliseconds);
                    testResult.TotalWaitTime += waitTime;
                    Thread.Sleep(waitTime);
                }
            }
        }

        public SimpleTestResult GetTestResult()
        {
            return testResult;
        }
    }
}