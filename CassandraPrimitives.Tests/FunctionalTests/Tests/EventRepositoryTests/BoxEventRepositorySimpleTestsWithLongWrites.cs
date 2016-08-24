using System;
using System.Collections.Generic;
using System.Linq;

using GroBuf;
using GroBuf.DataMembersExtracters;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.ClusterDeployment;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Configuration.ColumnFamilies;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Primitives;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Sharding;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.EventContents;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.LongWritesConnection;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Settings;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Tests.EventRepositoryTests
{
    public class BoxEventRepositorySimpleTestsWithLongWrites : BoxEventRepositoryTestBase
    {
        [Test, Ignore("Работает очень долго")]
        public void TestReadWrite()
        {
            boxIds = new[] {Guid.NewGuid().ToString(), Guid.NewGuid().ToString()};
            globalRandom = new Random(Guid.NewGuid().GetHashCode());

            using(var eventRepository = CreateBoxEventRepository((id, obj) => commonShard, 1))
            {
                var expectedEvents = new List<Event>();
                const int count = 1;
                for(var i = 0; i < count; ++i)
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

                for(var i = 0; i < expectedEvents.Count; ++i)
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
            globalRandom = new Random(Guid.NewGuid().GetHashCode());

            using(var eventRepository = CreateBoxEventRepository((id, obj) => commonShard, 0))
            {
                var expectedEvents = new List<Event>();
                const int count = 30;
                for(var i = 0; i < count; ++i)
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
                for(var i = 0; i < expectedEvents.Count; ++i)
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
            globalRandom = new Random(Guid.NewGuid().GetHashCode());

            using(var eventRepository = CreateBoxEventRepository((eventId, obj) =>
                                                                     {
                                                                         var keyDistributor = new KeyDistributor(64);
                                                                         return keyDistributor.Distribute(eventId.ScopeId).ToString();
                                                                     }, 0))
            {
                var expectedEvents = new List<Event>();
                const int count = 30;
                for(var i = 0; i < count; ++i)
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
                for(var i = 0; i < expectedEvents.Count; ++i)
                {
                    Console.WriteLine("Get events: from {0}.", i);
                    actualEvents = eventRepository.GetEvents(expectedEvents[i].EventInfo, shards).ToArray();
                    CheckEqualEvents(expectedEvents.Skip(i + 1).ToArray(), actualEvents);
                }
            }
        }

        private string GenerateScopeId()
        {
            return boxIds[globalRandom.Next(boxIds.Length)];
        }

        private const string commonShard = "commonShard";

        private IEventRepository CreateBoxEventRepository(Func<EventId, object, string> calculateShard, double timeoutInSeconds)
        {
            var serializer = new Serializer(new AllPropertiesExtractor());
            var cassandraSettings = SingleCassandraNodeSetUpFixture.Node.CreateSettings();
            var cassandraCluster = new CatalogueCassandraClusterWithLongWrites(new CassandraCluster(cassandraSettings), TimeSpan.FromSeconds(timeoutInSeconds));
            var eventTypeRegistry = new EventTypeRegistry();

            var factory = new EventRepositoryFactory(cassandraSettings.Endpoints, serializer, cassandraCluster, eventTypeRegistry);
            var eventRepositoryColumnFamilyFullNames = new EventRepositoryColumnFamilyFullNames(
                ColumnFamilies.ticksHolder,
                ColumnFamilies.eventLog,
                ColumnFamilies.eventLogAdditionalInfo,
                ColumnFamilies.remoteLock);
            var shardCalculator = new ShardCalculator(calculateShard);
            var result = factory.CreateEventRepository(shardCalculator, eventRepositoryColumnFamilyFullNames);
            return result;
        }

        private string[] boxIds;
        private Random globalRandom;
    }
}