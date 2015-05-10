using System;
using System.Threading;

using NUnit.Framework;

using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLockBase;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Tests.NewRemoteLockTests
{
    public class LockRentTests : NewLockTestsBase
    {
        public LockRentTests(LockType lockType)
            : base(lockType)
        {
        }

        [Test]
        public void TestGoodRentExtender()
        {
            ConfigureContainer(extendRentPeriod : 500);
            var lockCreator = container.Get<IRemoteLockCreator>();
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
            ConfigureContainer(extendRentPeriod : int.MaxValue);
            var lockCreator = container.Get<IRemoteLockCreator>();
            var lockId = Guid.NewGuid().ToString();
            lockCreator.Lock(lockId);
            Thread.Sleep(30000);
            IRemoteLock lock2;
            Assert.That(lockCreator.TryGetLock(lockId, out lock2), Is.True);
            lock2.Dispose();
        }
    }
}