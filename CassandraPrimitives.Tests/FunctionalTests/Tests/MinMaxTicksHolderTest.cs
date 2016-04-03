using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;

using GroboContainer.Infection;

using GroBuf;
using GroBuf.DataMembersExtracters;

using MoreLinq;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.ClusterDeployment;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.GlobalTicksHolder;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.Primitives;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Settings;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.SchemeActualizer;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Tests
{
    [TestFixture]
    public class MinMaxTicksHolderTest
    {
        [TestFixtureSetUp]
        public void TestFixtureSetUp()
        {
            var cassandraCluster = new CassandraCluster(StartSingleCassandraSetUp.Node.CreateSettings(IPAddress.Loopback));
            var cassandraSchemeActualizer = new CassandraSchemeActualizer(cassandraCluster, new MinMaxTicksHolderCassandraMetadataProvider(), new CassandraInitializerSettings());
            cassandraSchemeActualizer.AddNewColumnFamilies();

            var serializer = new Serializer(new AllPropertiesExtractor(), null, GroBufOptions.MergeOnRead);
            var minTicksConnection = cassandraCluster.RetrieveColumnFamilyConnection(minTicksColumnFamily.KeyspaceName, minTicksColumnFamily.ColumnFamilyName);
            minTicksHolder1 = new MinTicksHolder(serializer, minTicksConnection);
            minTicksHolder2 = new MinTicksHolder(serializer, minTicksConnection);
            var maxTicksConnection = cassandraCluster.RetrieveColumnFamilyConnection(minTicksColumnFamily.KeyspaceName, maxTicksColumnFamily.ColumnFamilyName);
            maxTicksHolder1 = new MaxTicksHolder(serializer, maxTicksConnection);
            maxTicksHolder2 = new MaxTicksHolder(serializer, maxTicksConnection);
        }

        [SetUp]
        public void SetUp()
        {
            minTicksHolder1.ResetInMemoryState();
            minTicksHolder2.ResetInMemoryState();
            maxTicksHolder1.ResetInMemoryState();
            maxTicksHolder2.ResetInMemoryState();
        }

        [Test]
        public void MinTicks()
        {
            var ticks = DateTime.UtcNow.Ticks;
            var key = Guid.NewGuid().ToString();
            Assert.That(minTicksHolder1.GetMinTicks(key), Is.Null);
            Assert.That(minTicksHolder2.GetMinTicks(key), Is.Null);
            Assert.That(minTicksHolder1.UpdateAndGetMinTicks(key, ticks), Is.EqualTo(ticks));
            Assert.That(minTicksHolder2.UpdateAndGetMinTicks(key, ticks - 2), Is.EqualTo(ticks - 2));
            Assert.That(minTicksHolder1.UpdateAndGetMinTicks(key, ticks - 1), Is.EqualTo(ticks - 2));
            Assert.That(minTicksHolder1.GetMinTicks(key), Is.EqualTo(ticks - 2));
            Assert.That(minTicksHolder2.GetMinTicks(key), Is.EqualTo(ticks - 2));
        }

        [Test]
        public void MaxTicks()
        {
            var ticks = DateTime.UtcNow.Ticks;
            var key = Guid.NewGuid().ToString();
            Assert.That(maxTicksHolder1.GetMaxTicks(key), Is.Null);
            Assert.That(maxTicksHolder2.GetMaxTicks(key), Is.Null);
            Assert.That(maxTicksHolder1.UpdateAndGetMaxTicks(key, ticks), Is.EqualTo(ticks));
            Assert.That(maxTicksHolder2.UpdateAndGetMaxTicks(key, ticks + 2), Is.EqualTo(ticks + 2));
            Assert.That(maxTicksHolder1.UpdateAndGetMaxTicks(key, ticks + 1), Is.EqualTo(ticks + 2));
            Assert.That(maxTicksHolder1.GetMaxTicks(key), Is.EqualTo(ticks + 2));
            Assert.That(maxTicksHolder2.GetMaxTicks(key), Is.EqualTo(ticks + 2));
        }

        [Test]
        public void ConcurrentUpdates()
        {
            var key = Guid.NewGuid().ToString();
            const int threadsCount = 8;
            const int countPerThread = 1000 * 1000;
            const int valuesCount = threadsCount * countPerThread;
            var rng = new Random(Guid.NewGuid().GetHashCode());
            var values = Enumerable.Range(0, valuesCount).Select(x => rng.Next(valuesCount)).ToList();
            var valuesByThread = values.Batch(countPerThread, Enumerable.ToArray).ToArray();
            var threads = new List<Thread>();
            var startSignal = new ManualResetEvent(false);
            for(var i = 0; i < threadsCount; i++)
            {
                var threadIndex = i;
                var thread = new Thread(() =>
                    {
                        startSignal.WaitOne();
                        var minTicksHolder = threadIndex % 2 == 0 ? minTicksHolder1 : minTicksHolder2;
                        var maxTicksHolder = threadIndex % 2 == 0 ? maxTicksHolder1 : maxTicksHolder2;
                        foreach(var value in valuesByThread[threadIndex])
                        {
                            minTicksHolder.UpdateMinTicks(key, value);
                            maxTicksHolder.UpdateMaxTicks(key, value);
                        }
                    });
                thread.Start();
                threads.Add(thread);
            }
            startSignal.Set();
            threads.ForEach(thread => thread.Join());
            Assert.That(minTicksHolder1.GetMinTicks(key), Is.EqualTo(values.Min()));
            Assert.That(minTicksHolder2.GetMinTicks(key), Is.EqualTo(values.Min()));
            Assert.That(maxTicksHolder1.GetMaxTicks(key), Is.EqualTo(values.Max()));
            Assert.That(maxTicksHolder2.GetMaxTicks(key), Is.EqualTo(values.Max()));
        }

        private MinTicksHolder minTicksHolder1, minTicksHolder2;
        private MaxTicksHolder maxTicksHolder1, maxTicksHolder2;
        private static readonly ColumnFamilyFullName minTicksColumnFamily = new ColumnFamilyFullName("CassandraPrimitives", "MinMaxTicksHolderTest_MinTicks");
        private static readonly ColumnFamilyFullName maxTicksColumnFamily = new ColumnFamilyFullName("CassandraPrimitives", "MinMaxTicksHolderTest_MaxTicks");

        [IgnoredImplementation]
        private class MinMaxTicksHolderCassandraMetadataProvider : ICassandraMetadataProvider
        {
            public ColumnFamilyFullName[] GetColumnFamilies()
            {
                return new[] {minTicksColumnFamily, maxTicksColumnFamily};
            }
        }
    }
}