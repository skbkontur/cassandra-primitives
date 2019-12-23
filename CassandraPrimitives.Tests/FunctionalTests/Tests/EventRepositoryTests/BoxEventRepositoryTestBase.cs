using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;

using GroBuf;
using GroBuf.DataMembersExtracters;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Clusters;

using SkbKontur.Cassandra.TimeBasedUuid;

using SKBKontur.Catalogue.CassandraPrimitives.EventLog;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Configuration.ColumnFamilies;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Primitives;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Sharding;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.EventContents;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.EventContents.Contents;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Helpers;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Settings;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.SchemeActualizer;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Tests.EventRepositoryTests
{
    [TestFixture]
    public abstract class BoxEventRepositoryTestBase
    {
        [OneTimeSetUp]
        public void TestFixtureSetUp()
        {
            cassandraClusterSettings = SingleCassandraNodeSetUpFixture.Node.CreateSettings();
            var initializerSettings = new CassandraInitializerSettings();
            cassandraSchemeActualizer = new CassandraSchemeActualizer(new CassandraCluster(cassandraClusterSettings, Logger.Instance), new CassandraMetaProvider(), initializerSettings);
            cassandraSchemeActualizer.AddNewColumnFamilies();
        }

        [SetUp]
        public virtual void SetUp()
        {
            cassandraSchemeActualizer.TruncateAllColumnFamilies();
            stopwatch = Stopwatch.StartNew();
            logDirectory = Guid.NewGuid().ToString();
            if (needLog)
            {
                if (!Directory.Exists(logDirectory))
                    Directory.CreateDirectory(logDirectory);
                Console.WriteLine("Reader logs path: {0}", logDirectory);
                logHashtable = new Hashtable();
            }
        }

        [TearDown]
        public virtual void TearDown()
        {
            Console.WriteLine("Test time: " + stopwatch.ElapsedMilliseconds);
            stopwatch.Stop();
        }

        protected void LogEventBatch(string fileName, Event[] events, string comment = null)
        {
            if (needLog)
                LogEventBatch(fileName, events.Select(x => x.EventInfo).ToArray(), comment);
        }

        protected void LogEventBatch(string fileName, EventInfo[] eventInfos, string comment = null)
        {
            if (needLog)
            {
                var eventTexts = eventInfos.Select(x => string.Format("{2:D20}. ScopeId={0} Id={1}", x.Id.ScopeId, x.Id.Id, x.Ticks)).ToArray();
                var title = string.IsNullOrEmpty(comment) ? "" : comment + "\n";

                var filePath = Path.Combine(logDirectory, fileName + ".txt");
                var text = title + string.Join("\n", eventTexts) + "\n\n";

                if (!logHashtable.ContainsKey(filePath) || ((int)logHashtable[filePath] != text.GetHashCode()))
                {
                    File.AppendAllText(filePath, text);

                    lock (logLockObject)
                    {
                        logHashtable[filePath] = text.GetHashCode();
                    }
                }
            }
        }

        protected virtual IEventRepository CreateBoxEventRepository(Func<EventId, object, string> calculateShard)
        {
            var serializer = new Serializer(new AllPropertiesExtractor());
            var cassandraCluster = new CassandraCluster(cassandraClusterSettings, Logger.Instance);
            var eventTypeRegistry = new EventTypeRegistry();

            var factory = new EventRepositoryFactory(serializer, cassandraCluster, eventTypeRegistry, Logger.Instance);
            var eventRepositoryColumnFamilyFullNames = new EventRepositoryColumnFamilyFullNames(
                ColumnFamilies.eventLog,
                ColumnFamilies.eventLogAdditionalInfo,
                ColumnFamilies.remoteLock);
            var shardCalculator = new ShardCalculator(calculateShard);
            var eventRepository = factory.CreateEventRepository(shardCalculator, eventRepositoryColumnFamilyFullNames);
            return eventRepository;
        }

        protected static object GenerateEventContent()
        {
            object eventContent;
            switch (ThreadLocalRandom.Instance.Next(3))
            {
            case 0:
                {
                    eventContent = new OutboxRawMessageEventContent {EntityId = Guid.NewGuid().ToString()};
                    break;
                }
            case 1:
                {
                    eventContent = new InboxRawMessageEventContent {EntityId = Guid.NewGuid().ToString()};
                    break;
                }
            default:
                {
                    eventContent = new SentEventContent {EntityId = Guid.NewGuid().ToString(), TransportType = "AS2"};
                    break;
                }
            }
            return eventContent;
        }

        protected static void CheckEqualEvents(Event[] expectedEvents, Event[] actualEvents)
        {
            Assert.AreEqual(expectedEvents.Length, actualEvents.Length);
            if (needDetailedComparison)
                actualEvents.AssertArrayEqualsTo(expectedEvents);
            else
                actualEvents.AssertEqualsToUsingGrobuf(expectedEvents);
            Console.WriteLine("CheckEqualEvents finished");
        }

        protected void CheckEqualEvents(List<Event>[] expectedEventsBatches, Event[] actualEvents)
        {
            var expectedTotalEventsCount = expectedEventsBatches.Sum(x => x.Count);
            Console.WriteLine("Expected: " + expectedTotalEventsCount + ", actual: " + actualEvents.Length);
            foreach (var batch in expectedEventsBatches)
            {
                var hashSet = new HashSet<EventId>(batch.Select(x => x.EventInfo.Id).ToArray());
                CheckEqualEvents(batch.ToArray(), actualEvents.Where(x => hashSet.Contains(x.EventInfo.Id)).ToArray());
            }
            Assert.AreEqual(expectedTotalEventsCount, actualEvents.Length);
            Console.WriteLine("CheckEqualEvents finished");
        }

        private const bool needLog = false;
        private const bool needDetailedComparison = false;

        private CassandraSchemeActualizer cassandraSchemeActualizer;
        private ICassandraClusterSettings cassandraClusterSettings;
        private string logDirectory;
        private Stopwatch stopwatch;

        private readonly object logLockObject = new object();
        private Hashtable logHashtable;
    }
}