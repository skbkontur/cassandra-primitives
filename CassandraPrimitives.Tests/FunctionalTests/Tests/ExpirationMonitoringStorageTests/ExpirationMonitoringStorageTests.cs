using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;

using GroBuf;
using GroBuf.DataMembersExtracters;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.ClusterDeployment;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.ExpirationMonitoringStorage;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Logging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Settings;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.SchemeActualizer;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Tests.ExpirationMonitoringStorageTests
{
    [TestFixture]
    public class ExpirationMonitoringStorageTests
    {
        [SetUp]
        public void SetUp()
        {
            cassandraClusterSettings = StartSingleCassandraSetUp.Node.CreateSettings(IPAddress.Loopback);
            var initializerSettings = new CassandraInitializerSettings();
            cassandraSchemeActualizer = new CassandraSchemeActualizer(new CassandraCluster(cassandraClusterSettings), new CassandraMetaProvider(), initializerSettings);
            cassandraSchemeActualizer.AddNewColumnFamilies();
            storage = new ExpirationMonitoringStorage(new CassandraCluster(cassandraClusterSettings), new Serializer(new AllPropertiesExtractor()), ColumnFamilies.expirationMonitoring);
            Log4NetConfiguration.InitializeOnce();
        }

        [Test]
        public void TestEmptyRange()
        {
            var from = 0;
            var to = TimeSpan.FromDays(150).Ticks;
            var entries = storage.GetEntries(from, to);
            Assert.That(entries.Length, Is.EqualTo(0));
        }

        [Test]
        public void TestNonEmptyRange()
        {
            var entries = Enumerable.Range(0, 10).Select(x => CreateEntry()).ToArray();
            var ticks = Enumerable.Range(-1, 11).Select(x => TimeSpan.FromMinutes(x).Ticks).ToArray();

            for(var i = 0; i < 10; i++)
                storage.AddEntry(entries[i], ticks[i + 1]);

            for(var i = 0; i < ticks.Length; i++)
            {
                for(var j = i; j < ticks.Length; j++)
                    CheckResult(ticks[i], ticks[j], entries.Skip(i).Take(j - i).ToArray());
            }
        }

        [Test]
        public void TestDeleteEntry()
        {
            var entry = CreateEntry();
            var from = TimeSpan.FromMinutes(1).Ticks;
            var ticks = TimeSpan.FromMinutes(2).Ticks;
            var to = TimeSpan.FromMinutes(3).Ticks;

            storage.AddEntry(entry, ticks);
            CheckResult(from, to, new[] {entry});
            storage.DeleteEntry(entry, ticks);
            CheckResult(from, to, new ExpiringObjectMeta[0]);
        }

        private void CheckResult(long from, long to, ExpiringObjectMeta[] expected)
        {
            var actual = storage.GetEntries(from, to);
            Assert.That(actual.Length, Is.EqualTo(expected.Length));
            Assert.That(actual, Is.EquivalentTo(expected).Using(new MetaComparer()));
        }

        private ExpiringObjectMeta CreateEntry()
        {
            return new ExpiringObjectMeta
            {
                Keyspace = Guid.NewGuid().ToString(),
                ColumnFamily = Guid.NewGuid().ToString(),
                Row = Guid.NewGuid().ToString(),
                Column = Guid.NewGuid().ToString(),
            };
        }

        private ICassandraClusterSettings cassandraClusterSettings;
        private CassandraSchemeActualizer cassandraSchemeActualizer;
        private IExpirationMonitoringStorage storage;

        private class MetaComparer : IEqualityComparer<ExpiringObjectMeta>
        {
            public bool Equals(ExpiringObjectMeta x, ExpiringObjectMeta y)
            {
                return x.Keyspace == y.Keyspace && x.ColumnFamily == y.ColumnFamily && x.Row == y.Row && x.Column == y.Column;
            }

            public int GetHashCode(ExpiringObjectMeta obj)
            {
                return string.Format("{0}_{1}_{2}_{3}", obj.Keyspace, obj.ColumnFamily, obj.Row, obj.Column).GetHashCode();
            }
        }
    }
}