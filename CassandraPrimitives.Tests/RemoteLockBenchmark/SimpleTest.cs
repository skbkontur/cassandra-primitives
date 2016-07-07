using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;

using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark
{
    public class SimpleTest : ITest<SimpleTestResult>
    {
        public SimpleTest(TestConfiguration configuration, int processInd, IRemoteLockGetter remoteLockGetter)
        {
            this.configuration = configuration;
            this.processInd = processInd;
            locker = remoteLockGetter.Get(1).Single();
            lockId = Guid.NewGuid().ToString();
            rand = new Random(Guid.NewGuid().GetHashCode());
            testResult = new SimpleTestResult();
        }

        public void SetUp()
        {
            stopwatch = Stopwatch.StartNew();
        }

        public void DoWorkInSingleThread(int threadInd)
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

        public void TearDown()
        {
            testResult.TotalTimeSpent = stopwatch.ElapsedMilliseconds;
        }

        public SimpleTestResult GetTestResult()
        {
            return testResult;
        }

        private readonly TestConfiguration configuration;
        private readonly int processInd;
        private readonly IRemoteLockCreator locker;
        private readonly string lockId;
        private readonly Random rand;
        private readonly SimpleTestResult testResult;
        private Stopwatch stopwatch;
    }
}