using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

using NUnit.Framework;

using SKBKontur.Catalogue.CassandraPrimitives.EventLog;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Primitives;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Tests.EventRepositoryTests
{
    [TestFixture(true)]
    [TestFixture(false)]
    public class BoxEventRepositorySimpleTests : BoxEventRepositoryTestBase
    {
        public BoxEventRepositorySimpleTests(bool asyncWrite)
        {
            this.asyncWrite = asyncWrite;
        }

        public override void SetUp()
        {
            base.SetUp();
            eventRepository = CreateBoxEventRepository((id, obj) => commonShard);
            boxIds = new[] {Guid.NewGuid().ToString(), Guid.NewGuid().ToString()};
            globalRandom = new Random(Guid.NewGuid().GetHashCode());
        }

        public override void TearDown()
        {
            eventRepository.Dispose();
            base.TearDown();
        }

        private EventInfo AddEvent(string scopeId, object eventContent)
        {
            if (asyncWrite)
                return eventRepository.AddEventsAsync(new[] {new KeyValuePair<string, object>(scopeId, eventContent),}).Result.First();
            return eventRepository.AddEvent(scopeId, eventContent);
        }

        [Test]
        public void TestNewExclusiveEventInfoIfEmpty()
        {
            var guid1 = Guid.NewGuid().ToString();
            var guid2 = Guid.NewGuid().ToString();
            using (var er = CreateBoxEventRepository((x, y) =>
                {
                    if (x.ScopeId == guid1) return "1";
                    if (x.ScopeId == guid2) return "2";
                    return "3";
                }))
            {
                var ev1 = er.AddEvent(guid1, GenerateEventContent());
                var ev2 = er.AddEvent(guid1, GenerateEventContent());
                var ev3 = er.AddEvent(guid1, GenerateEventContent());
                EventInfo eventInfo;
                Assert.That(er.GetEventsWithUnstableZone(null, new[] {"2", "3"}, out eventInfo).ToArray().Length == 0);
                Assert.That(eventInfo.CompareTo(ev3) == 0);
                Assert.That(er.GetEventsWithUnstableZone(ev1, new[] {"2", "3"}, out eventInfo).ToArray().Length == 0);
                Assert.That(eventInfo.CompareTo(ev3) == 0);
                Assert.That(er.GetEventsWithUnstableZone(ev2, new[] {"2", "3"}, out eventInfo).ToArray().Length == 0);
                Assert.That(eventInfo.CompareTo(ev3) == 0);
                Assert.That(er.GetEventsWithUnstableZone(ev3, new[] {"2", "3"}, out eventInfo).ToArray().Length == 0);
                Assert.That(eventInfo.CompareTo(ev3) == 0);
            }
        }

        [Test]
        public void TestReadEmpty()
        {
            var events = eventRepository.GetEventsWithUnstableZone(null, new[] {commonShard}).ToArray();
            Assert.That(events.Length == 0);
        }

        [Test]
        public void TestReadWrite()
        {
            var expectedEvents = new List<Event>();
            const int count = 20;
            for (var i = 0; i < count; ++i)
            {
                var scopeId = GenerateScopeId();
                var eventContent = GenerateEventContent();
                var eventInfo = AddEvent(scopeId, eventContent);
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

        [Test]
        public void TestRead()
        {
            var actualEvents = eventRepository.GetEvents(null, new[] {commonShard}).ToArray();
            Assert.That(actualEvents, Is.Empty);
        }

        [Test]
        public void TestManyWrite()
        {
            var expectedEvents = new List<Event>();
            var watch = Stopwatch.StartNew();
            watch.Start();
            for (var i = 0; i < 10000; ++i)
            {
                var boxEvent = GenerateEventContent();
                var eventInfo = AddEvent(GenerateScopeId(), boxEvent);
                expectedEvents.Add(new Event
                    {
                        EventInfo = eventInfo,
                        EventContent = boxEvent,
                    });
            }
            watch.Stop();
            Console.WriteLine(watch.ElapsedMilliseconds);

            var actualEvents = eventRepository.GetEvents(null, new[] {commonShard}).ToArray();
            CheckEqualEvents(expectedEvents.ToArray(), actualEvents);
        }

        private string GenerateScopeId()
        {
            return boxIds[globalRandom.Next(boxIds.Length)];
        }

        private const string commonShard = "commonShard";
        private readonly bool asyncWrite;

        private IEventRepository eventRepository;
        private string[] boxIds;
        private Random globalRandom;
    }
}