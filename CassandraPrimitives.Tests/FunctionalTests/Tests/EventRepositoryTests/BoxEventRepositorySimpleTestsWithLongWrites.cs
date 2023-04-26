using System;
using System.Collections.Generic;
using System.Linq;

using CassandraPrimitives.Tests.FunctionalTests.EventContents;
using CassandraPrimitives.Tests.FunctionalTests.LongWritesConnection;
using CassandraPrimitives.Tests.FunctionalTests.Settings;

using GroBuf;
using GroBuf.DataMembersExtracters;

using NUnit.Framework;

using SkbKontur.Cassandra.Primitives.EventLog;
using SkbKontur.Cassandra.Primitives.EventLog.Configuration.ColumnFamilies;
using SkbKontur.Cassandra.Primitives.EventLog.Primitives;
using SkbKontur.Cassandra.Primitives.EventLog.Profiling;
using SkbKontur.Cassandra.Primitives.EventLog.Sharding;
using SkbKontur.Cassandra.ThriftClient.Clusters;
using SkbKontur.Cassandra.TimeBasedUuid;

namespace CassandraPrimitives.Tests.FunctionalTests.Tests.EventRepositoryTests
{
    public class BoxEventRepositorySimpleTestsWithLongWrites : BoxEventRepositoryTestBase
    {
        [Test, Ignore("Работает очень долго")]
        public void TestReadWrite()
        {
            boxIds = new[] {Guid.NewGuid().ToString(), Guid.NewGuid().ToString()};

            using (var eventRepository = CreateBoxEventRepository((id, obj) => commonShard, 1))
            {
                var expectedEvents = new List<Event>();
                const int count = 1;
                for (var i = 0; i < count; ++i)
                {
                    var scopeId = GenerateScopeId();
                    var eventContent = GenerateEventContent();
                    var eventInfo = eventRepository.AddEvent(scopeId, eventContent);
                    Assert.AreEqual(scopeId, eventInfo.Id.ScopeId);

                    expectedEvents.Add(new Event
                        {
                            EventInfo = eventInfo,
                            EventContent = eventContent,
                        });
                }

                var actualEvents = eventRepository.GetEvents(null, new[] {commonShard}).ToArray();
                CheckEqualEvents(expectedEvents.ToArray(), actualEvents);

                for (var i = 0; i < expectedEvents.Count; ++i)
                {
                    actualEvents = eventRepository.GetEvents(expectedEvents[i].EventInfo, new[] {commonShard}).ToArray();
                    CheckEqualEvents(expectedEvents.Skip(i + 1).ToArray(), actualEvents);
                }
            }
        }

        [Test]
        public void TestWriteAndRead1Shard()
        {
            boxIds = new[] {Guid.NewGuid().ToString(), Guid.NewGuid().ToString()};

            using (var eventRepository = CreateBoxEventRepository((id, obj) => commonShard, 0))
            {
                var expectedEvents = new List<Event>();
                const int count = 30;
                for (var i = 0; i < count; ++i)
                {
                    Console.WriteLine("Start write event {0}", i);
                    var scopeId = GenerateScopeId();
                    var eventContent = GenerateEventContent();
                    var eventInfo = eventRepository.AddEvent(scopeId, eventContent);
                    Assert.AreEqual(scopeId, eventInfo.Id.ScopeId);

                    expectedEvents.Add(new Event
                        {
                            EventInfo = eventInfo,
                            EventContent = eventContent,
                        });
                }

                Console.WriteLine("Get events: all.");
                var actualEvents = eventRepository.GetEvents(null, new[] {commonShard}).ToArray();
                CheckEqualEvents(expectedEvents.ToArray(), actualEvents);
                for (var i = 0; i < expectedEvents.Count; ++i)
                {
                    Console.WriteLine("Get events: from {0}.", i);
                    actualEvents = eventRepository.GetEvents(expectedEvents[i].EventInfo, new[] {commonShard}).ToArray();
                    CheckEqualEvents(expectedEvents.Skip(i + 1).ToArray(), actualEvents);
                }
            }
        }

        [Test]
        public void TestWriteAndRead64Shard()
        {
            boxIds = new[] {Guid.NewGuid().ToString(), Guid.NewGuid().ToString()};

            using (var eventRepository = CreateBoxEventRepository((eventId, obj) =>
                {
                    var keyDistributor = new KeyDistributor(64);
                    return keyDistributor.Distribute(eventId.ScopeId).ToString();
                }, 0))
            {
                var expectedEvents = new List<Event>();
                const int count = 30;
                for (var i = 0; i < count; ++i)
                {
                    Console.WriteLine("Start write event {0}", i);
                    var scopeId = GenerateScopeId();
                    var eventContent = GenerateEventContent();
                    var eventInfo = eventRepository.AddEvent(scopeId, eventContent);
                    Assert.AreEqual(scopeId, eventInfo.Id.ScopeId);

                    expectedEvents.Add(new Event
                        {
                            EventInfo = eventInfo,
                            EventContent = eventContent,
                        });
                }

                var shards = new string[64].Select((x, idx) => (idx.ToString())).ToArray();
                Console.WriteLine("Get events: all.");
                var actualEvents = eventRepository.GetEvents(null, shards).ToArray();
                CheckEqualEvents(expectedEvents.ToArray(), actualEvents);
                for (var i = 0; i < expectedEvents.Count; ++i)
                {
                    Console.WriteLine("Get events: from {0}.", i);
                    actualEvents = eventRepository.GetEvents(expectedEvents[i].EventInfo, shards).ToArray();
                    CheckEqualEvents(expectedEvents.Skip(i + 1).ToArray(), actualEvents);
                }
            }
        }

        private string GenerateScopeId()
        {
            return boxIds[ThreadLocalRandom.Instance.Next(boxIds.Length)];
        }

        private const string commonShard = "commonShard";

        private IEventRepository CreateBoxEventRepository(Func<EventId, object, string> calculateShard, double timeoutInSeconds)
        {
            var serializer = new Serializer(new AllPropertiesExtractor());
            var cassandraSettings = LocalCassandraSettingsFactory.CreateSettings();
            var cassandraCluster = new CatalogueCassandraClusterWithLongWrites(new CassandraCluster(cassandraSettings, Logger.Instance), TimeSpan.FromSeconds(timeoutInSeconds));
            var eventTypeRegistry = new EventTypeRegistry();

            var factory = new EventRepositoryFactory(serializer, cassandraCluster, eventTypeRegistry, Logger.Instance);
            var eventRepositoryColumnFamilyFullNames = new EventRepositoryColumnFamilyFullNames(
                ColumnFamilies.eventLog,
                ColumnFamilies.eventLogAdditionalInfo,
                ColumnFamilies.remoteLock);
            var shardCalculator = new ShardCalculator(calculateShard);
            var result = factory.CreateEventRepository(shardCalculator, eventRepositoryColumnFamilyFullNames, new EventLogNullProfiler(), TimeSpan.FromDays(1));
            return result;
        }

        private string[] boxIds;
    }
}