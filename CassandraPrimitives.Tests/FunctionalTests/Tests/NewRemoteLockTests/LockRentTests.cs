using System;
using System.Threading;

using NUnit.Framework;

using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithCassanrdaTTL;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Settings;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Tests.NewRemoteLockTests
{
    [TestFixture]
    public class LockRentTests : NewLockTestsBase
    {
        [Test]
        public void TestGoodRentExtender()
        {
            ConfigureContainer(new RemoteLockSettings(ColumnFamilies.newRemoteLock.KeyspaceName, ColumnFamilies.newRemoteLock.ColumnFamilyName, 1000, 3000, 500, false));
            var lockCreator = container.Get<NewRemoteLockCreator>();
            var lockId = Guid.NewGuid().ToString();
            var lock1 = lockCreator.Lock(lockId);
            Thread.Sleep(10000);
            IRemoteLock lock2;
            Assert.That(lockCreator.TryGetLock(lockId, out lock2), Is.False);
            lock1.Dispose();
            Thread.Sleep(2000);
            Assert.That(lockCreator.TryGetLock(lockId, out lock2), Is.True);
            lock2.Dispose();
        }

        [Test]
        public void TestBadRentExtender()
        {
            ConfigureContainer(new RemoteLockSettings(ColumnFamilies.newRemoteLock.KeyspaceName, ColumnFamilies.newRemoteLock.ColumnFamilyName, 1000, 3000, int.MaxValue, false));
            var lockCreator = container.Get<NewRemoteLockCreator>();
            var lockId = Guid.NewGuid().ToString();
            lockCreator.Lock(lockId);
            Thread.Sleep(30000);
            IRemoteLock lock2;
            Assert.That(lockCreator.TryGetLock(lockId, out lock2), Is.True);
            lock2.Dispose();
        }
    }
}