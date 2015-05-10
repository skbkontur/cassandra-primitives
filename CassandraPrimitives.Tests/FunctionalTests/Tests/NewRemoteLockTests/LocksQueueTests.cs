using System;
using System.Linq;

using NUnit.Framework;

using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithCassanrdaTTL;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithCassanrdaTTL.QueueStorage;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Settings;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Tests.NewRemoteLockTests
{
    [TestFixture]
    public class LocksQueueTests : NewLockTestsBase
    {
        [Test]
        public void TestQueueInOneRow()
        {
            ConfigureContainer(new RemoteLockSettings(ColumnFamilies.newRemoteLock.KeyspaceName, ColumnFamilies.newRemoteLock.ColumnFamilyName));
            var queueStorage = container.Get<IQueueStorage>();
            var lockId = GetGuid();
            var threadId1 = GetGuid();
            var ts1 = 2;
            var threadId2 = GetGuid();
            var ts2 = 1;
            var firstRowName = queueStorage.Add(lockId, threadId1, ts1);
            Assert.That(queueStorage.GetFirstElement(lockId), Is.EqualTo(threadId1));
            var secondRowName = queueStorage.Add(lockId, threadId2, ts2);
            Assert.That(firstRowName, Is.EqualTo(secondRowName));
            Assert.That(queueStorage.GetFirstElement(lockId), Is.EqualTo(threadId2));
            Assert.That(queueStorage.GetFirstElement(GetGuid()), Is.Null);
            queueStorage.Remove(lockId, firstRowName, threadId1, ts1);
            Assert.That(queueStorage.GetFirstElement(lockId), Is.EqualTo(threadId2));
            queueStorage.Remove(lockId, secondRowName, threadId2, ts2);
            Assert.That(queueStorage.GetFirstElement(lockId), Is.Null);
        }

        [Test]
        public void TestQueueInManyRows()
        {
            ConfigureContainer(new RemoteLockSettings(ColumnFamilies.newRemoteLock.KeyspaceName, ColumnFamilies.newRemoteLock.ColumnFamilyName, 2));
            var queueStorage = container.Get<IQueueStorage>();
            var lockId = GetGuid();
            var elements = Enumerable.Range(0, 1000).Select(x => new Zzz {ThreadId = GetGuid(), Timestamp = x}).ToArray();
            foreach(var element in elements)
                element.RowName = queueStorage.Add(lockId, element.ThreadId, element.Timestamp);
            for(var i = 0; i < 999; i++)
            {
                if(i % 2 == 0)
                    Assert.That(elements[i].RowName, Is.EqualTo(elements[i + 1].RowName));
                else
                    Assert.That(elements[i].RowName, Is.Not.EqualTo(elements[i + 1].RowName));
            }
            foreach(var element in elements)
            {
                Assert.That(queueStorage.GetFirstElement(lockId), Is.EqualTo(element.ThreadId));
                queueStorage.Remove(lockId, element.RowName, element.ThreadId, element.Timestamp);
            }
            Assert.That(queueStorage.GetFirstElement(lockId), Is.Null);
        }

        private string GetGuid()
        {
            return Guid.NewGuid().ToString();
        }

        private class Zzz
        {
            public string ThreadId { get; set; }
            public string RowName { get; set; }
            public long Timestamp { get; set; }
        }
    }
}