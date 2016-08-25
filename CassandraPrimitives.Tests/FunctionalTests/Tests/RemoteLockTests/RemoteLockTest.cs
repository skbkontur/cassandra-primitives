using System;
using System.Threading;

using log4net;

using NUnit.Framework;

using SKBKontur.Catalogue.CassandraPrimitives.CasRemoteLock;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Tests.RemoteLockTests
{
    public class RemoteLockTest : RemoteLockTestBase
    {
        public override void SetUp()
        {
            base.SetUp();
            logger = LogManager.GetLogger(typeof(RemoteLockTest));
            casRemoteLockProvider = container.Get<CasRemoteLockProvider>();
            casRemoteLockProvider.ActualiseTables();
            casRemoteLockProvider.InitPreparedStatements();
        }

        [TestCase(LocalRivalOptimization.Disabled)]
        [TestCase(LocalRivalOptimization.Enabled)]
        public void StressTest(LocalRivalOptimization localRivalOptimization)
        {
            DoTestIncrementDecrementLock(30, TimeSpan.FromSeconds(60), localRivalOptimization);
        }

        [TestCase(LocalRivalOptimization.Disabled)]
        [TestCase(LocalRivalOptimization.Enabled)]
        public void TestIncrementDecrementLock(LocalRivalOptimization localRivalOptimization)
        {
            DoTestIncrementDecrementLock(10, TimeSpan.FromSeconds(10), localRivalOptimization);
        }

        private void DoTestIncrementDecrementLock(int threadCount, TimeSpan runningTimeInterval, LocalRivalOptimization localRivalOptimization)
        {
            var remoteLockCreators = PrepareRemoteLockCreators(threadCount, localRivalOptimization, casRemoteLockProvider);

            for(var i = 0; i < threadCount; i++)
                AddThread(IncrementDecrementAction, remoteLockCreators[i]);
            RunThreads(runningTimeInterval);
            JoinThreads();

            DisposeRemoteLockCreators(remoteLockCreators);
        }

        private void IncrementDecrementAction(IRemoteLockCreator lockCreator, Random random)
        {
            try
            {
                var remoteLock = lockCreator.Lock(lockId);
                using(remoteLock)
                {
                    logger.Info("MakeLock with threadId: " + remoteLock.ThreadId);
                    Thread.Sleep(1000);
                    CheckLocks(remoteLock.ThreadId);
                    Assert.AreEqual(0, ReadX());
                    logger.Info("Increment");
                    Interlocked.Increment(ref x);
                    logger.Info("Decrement");
                    Interlocked.Decrement(ref x);
                }
            }
            catch(Exception e)
            {
                logger.Error(e);
                throw;
            }
        }

        private int ReadX()
        {
            return Interlocked.CompareExchange(ref x, 0, 0);
        }

        private void CheckLocks(string threadId)
        {
            try
            {
                var owner = casRemoteLockProvider.CreateLocker().GetLockOwner(lockId);
                Assert.That(owner.Length == 1);
                Assert.AreEqual(threadId, owner[0]);
            }
            catch(Exception e)
            {
                logger.Error(e);
                throw;
            }
        }

        private const string lockId = "IncDecLock";
        private int x;
        private ILog logger;
        private CasRemoteLockProvider casRemoteLockProvider;
        //private CassandraRemoteLockImplementation remoteLockImplementation;
    }
}