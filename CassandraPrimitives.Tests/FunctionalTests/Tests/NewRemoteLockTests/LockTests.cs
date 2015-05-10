using System;

using NUnit.Framework;

using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithCassanrdaTTL;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Settings;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Tests.NewRemoteLockTests
{
    [TestFixture]
    public class LockTests : NewLockTestsBase
    {
        [Test]
        public void TestTryLock()
        {
            ConfigureContainer(new RemoteLockSettings(ColumnFamilies.newRemoteLock.KeyspaceName, ColumnFamilies.newRemoteLock.ColumnFamilyName));
            remoteLockCreator = container.Get<NewRemoteLockCreator>();
            var lockId = Guid.NewGuid().ToString();
            IRemoteLock lock1, lock2;
            Assert.That(remoteLockCreator.TryGetLock(lockId, out lock1), Is.True);
            Assert.That(lock1, Is.Not.Null);
            Assert.That(remoteLockCreator.TryGetLock(lockId, out lock2), Is.False);
            Assert.That(lock2, Is.Null);
            lock1.Dispose();
            Assert.That(remoteLockCreator.TryGetLock(lockId, out lock2), Is.True);
            Assert.That(lock2, Is.Not.Null);
            lock2.Dispose();
        }

        [Test]
        public void TestLock()
        {
            ConfigureContainer(new RemoteLockSettings(ColumnFamilies.newRemoteLock.KeyspaceName, ColumnFamilies.newRemoteLock.ColumnFamilyName));
            remoteLockCreator = container.Get<NewRemoteLockCreator>();
            var lockId = Guid.NewGuid().ToString();
            IRemoteLock lock1, lock2;
            lock1 = remoteLockCreator.Lock(lockId);
            Assert.That(remoteLockCreator.TryGetLock(lockId, out lock2), Is.False);
            lock1.Dispose();
            Assert.That(remoteLockCreator.TryGetLock(lockId, out lock2), Is.True);
            lock2.Dispose();
        }

        [Test]
        public void TestDifferentLockIds()
        {
            ConfigureContainer(new RemoteLockSettings(ColumnFamilies.newRemoteLock.KeyspaceName, ColumnFamilies.newRemoteLock.ColumnFamilyName));
            remoteLockCreator = container.Get<NewRemoteLockCreator>();
            var lockId1 = Guid.NewGuid().ToString();
            var lockId2 = Guid.NewGuid().ToString();
            var lockId3 = Guid.NewGuid().ToString();
            IRemoteLock lock1, lock2, lock3;
            Assert.That(remoteLockCreator.TryGetLock(lockId1, out lock1), Is.True);
            Assert.That(remoteLockCreator.TryGetLock(lockId2, out lock2), Is.True);
            Assert.That(remoteLockCreator.TryGetLock(lockId3, out lock3), Is.True);
            lock1.Dispose();
            lock2.Dispose();
            lock3.Dispose();
        }

        private IRemoteLockCreator remoteLockCreator;
    }
}