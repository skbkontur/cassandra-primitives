﻿using System;
using System.Threading;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Settings;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.SchemeActualizer;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Tests.RemoteLockTests
{
    [TestFixture]
    public class BasicRemoteLockerTests
    {
        [TestFixtureSetUp]
        public void TestFixtureSetUp()
        {
            var cassandraCluster = new CassandraCluster(CassandraClusterSettings.ForNode(StartSingleCassandraSetUp.Node));
            var cassandraSchemeActualizer = new CassandraSchemeActualizer(cassandraCluster, new CassandraMetaProvider(), new CassandraInitializerSettings());
            cassandraSchemeActualizer.AddNewColumnFamilies();
        }

        [TestCase(true)]
        [TestCase(false)]
        public void TryLock_SingleLockId(bool useSingleLockKeeperThread)
        {
            using(var tester = new RemoteLockerTester(useSingleLockKeeperThread))
            {
                var lockId = Guid.NewGuid().ToString();
                IRemoteLock lock1, lock2;
                Assert.That(tester.TryGetLock(lockId, out lock1), Is.True);
                Assert.That(lock1, Is.Not.Null);
                Assert.That(tester.TryGetLock(lockId, out lock2), Is.False);
                Assert.That(lock2, Is.Null);
                lock1.Dispose();
                Assert.That(tester.TryGetLock(lockId, out lock2), Is.True);
                Assert.That(lock2, Is.Not.Null);
                lock2.Dispose();
            }
        }

        [TestCase(true)]
        [TestCase(false)]
        public void TryLock_DifferentLockIds(bool useSingleLockKeeperThread)
        {
            using(var tester = new RemoteLockerTester(useSingleLockKeeperThread))
            {
                var lockId1 = Guid.NewGuid().ToString();
                var lockId2 = Guid.NewGuid().ToString();
                var lockId3 = Guid.NewGuid().ToString();
                IRemoteLock lock1, lock2, lock3;
                Assert.That(tester.TryGetLock(lockId1, out lock1), Is.True);
                Assert.That(tester.TryGetLock(lockId2, out lock2), Is.True);
                Assert.That(tester.TryGetLock(lockId3, out lock3), Is.True);
                lock1.Dispose();
                lock2.Dispose();
                lock3.Dispose();
            }
        }

        [TestCase(true)]
        [TestCase(false)]
        public void Lock(bool useSingleLockKeeperThread)
        {
            using(var tester = new RemoteLockerTester(useSingleLockKeeperThread))
            {
                var lockId = Guid.NewGuid().ToString();
                var lock1 = tester.Lock(lockId);
                IRemoteLock lock2;
                Assert.That(tester.TryGetLock(lockId, out lock2), Is.False);
                lock1.Dispose();
                Assert.That(tester.TryGetLock(lockId, out lock2), Is.True);
                lock2.Dispose();
            }
        }

        [TestCase(true)]
        [TestCase(false)]
        public void LockIsKeptAlive_Success(bool useSingleLockKeeperThread)
        {
            var config = new RemoteLockerTesterConfig
                {
                    LockCreatorsCount = 2,
                    LocalRivalOptimization = LocalRivalOptimization.Disabled,
                    LockTtl = TimeSpan.FromSeconds(10),
                    KeepLockAliveInterval = TimeSpan.FromSeconds(5),
                    CassandraClusterSettings = CassandraClusterSettings.ForNode(StartSingleCassandraSetUp.Node, attempts : 1, timeout : TimeSpan.FromSeconds(1)),
                };
            using(var tester = new RemoteLockerTester(useSingleLockKeeperThread, config))
            {
                var lockId = Guid.NewGuid().ToString();
                var lock1 = tester[0].Lock(lockId);
                Thread.Sleep(TimeSpan.FromSeconds(12)); // waiting in total: 12 = 1*1 + 10 + 1 sec
                IRemoteLock lock2;
                Assert.That(tester[1].TryGetLock(lockId, out lock2), Is.False);
                lock1.Dispose();
                Assert.That(tester[1].TryGetLock(lockId, out lock2), Is.True);
                lock2.Dispose();
            }
        }

        [TestCase(true)]
        [TestCase(false)]
        public void LockIsKeptAlive_Failure(bool useSingleLockKeeperThread)
        {
            var config = new RemoteLockerTesterConfig
                {
                    LockCreatorsCount = 2,
                    LocalRivalOptimization = LocalRivalOptimization.Disabled,
                    LockTtl = TimeSpan.FromSeconds(5),
                    KeepLockAliveInterval = TimeSpan.FromSeconds(10),
                    CassandraClusterSettings = CassandraClusterSettings.ForNode(StartSingleCassandraSetUp.Node, attempts : 1, timeout : TimeSpan.FromSeconds(1)),
                };
            using(var tester = new RemoteLockerTester(useSingleLockKeeperThread, config))
            {
                var lockId = Guid.NewGuid().ToString();
                var lock1 = tester[0].Lock(lockId);
                Thread.Sleep(TimeSpan.FromSeconds(3));
                IRemoteLock lock2;
                Assert.That(tester[1].TryGetLock(lockId, out lock2), Is.False);
                Thread.Sleep(TimeSpan.FromSeconds(4)); // waiting in total: 3 + 4 = 1*1 + 5 + 1 sec
                Assert.That(tester[1].TryGetLock(lockId, out lock2), Is.True);
                lock2.Dispose();
                lock1.Dispose();
            }
        }
    }
}