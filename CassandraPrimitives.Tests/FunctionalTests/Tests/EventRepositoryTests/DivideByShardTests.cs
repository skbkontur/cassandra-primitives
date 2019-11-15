using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;

using NUnit.Framework;

using SkbKontur.Cassandra.TimeBasedUuid;

using SKBKontur.Catalogue.CassandraPrimitives.EventLog;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Primitives;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Sharding;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Tests.EventRepositoryTests
{
    public class DivideByShardTests : BoxEventRepositoryTestBase
    {
        public override void SetUp()
        {
            base.SetUp();
            keyDistributor = new KeyDistributor(shardsCount);
            globalEventRepository = CreateBoxEventRepository();
        }

        public override void TearDown()
        {
            globalEventRepository.Dispose();
            base.TearDown();
        }

        [Test]
        public void DoTestMultiThread()
        {
            totalWrittenEvents = 0;
            const int count = 5;
            Assert.That(count <= shardsCount);

            var writeThreads = new List<Thread>();
            var writtenEventsByThread = new List<Event>[count];
            for (var i = 0; i < count; ++i)
            {
                var currentList = writtenEventsByThread[i] = new List<Event>();
                var idx = i;
                var threadId = i;
                writeThreads.Add(new Thread(() => ThreadWriter(globalEventRepository, currentList, threadId, GetShardsPart(idx, count))));
            }

            var readThreads = new List<Thread>();
            var readEventsByThread = new SortedSet<Event>[count];
            for (var i = 0; i < count; ++i)
            {
                var currentSet = readEventsByThread[i] = new SortedSet<Event>(new EventComparer());
                var idx = i;
                var threadId = i;
                readThreads.Add(new Thread(() => ThreadReader(globalEventRepository, currentSet, threadId, GetShardsPart(idx, count))));
            }

            var watch = Stopwatch.StartNew();
            watch.Start();
            for (var i = 0; i < count; ++i)
            {
                writeThreads[i].Start();
                readThreads[i].Start();
            }

            for (var i = 0; i < count; ++i)
                writeThreads[i].Join();

            var elapsed = watch.Elapsed;
            Console.WriteLine(elapsed.TotalMilliseconds);
            totalWrittenEvents = writtenEventsByThread.Sum(x => x.Count);
            for (var i = 0; i < count; ++i)
                readThreads[i].Join();
            watch.Stop();
            Console.WriteLine(watch.ElapsedMilliseconds);

            var actualBoxEvents = globalEventRepository.GetEvents(null, GetAllShards()).ToArray();
            CheckEqualEvents(writtenEventsByThread, actualBoxEvents);
            for (var i = 0; i < count; i++)
            {
                var readEvents = readEventsByThread[i];
                var writtenEvents = writtenEventsByThread[i];
                CheckEqualEvents(writtenEvents.ToArray(), readEvents.ToArray());
            }
        }

        [Test]
        public void DoTestMultipleRepositoriesMultiThread()
        {
            const int numberOfRepositories = 5;
            var boxEventRepositories = new List<IEventRepository>();
            for (var i = 0; i < numberOfRepositories; ++i)
                boxEventRepositories.Add(CreateBoxEventRepository());
            totalWrittenEvents = 0;
            const int count = 20;

            var writeThreads = new List<Thread>();
            var writtenEventsByThread = new List<Event>[count];
            for (var i = 0; i < count; ++i)
            {
                var currentList = writtenEventsByThread[i] = new List<Event>();
                var idx = i;
                var threadId = i;
                writeThreads.Add(new Thread(() => ThreadWriter(boxEventRepositories[idx % numberOfRepositories], currentList, threadId, GetShardsPart(idx, count))));
            }

            var readThreads = new List<Thread>();
            var readEventsByThread = new SortedSet<Event>[count];
            for (var i = 0; i < count; ++i)
            {
                var currentList = readEventsByThread[i] = new SortedSet<Event>(new EventComparer());
                var idx = i;
                var threadId = i;
                readThreads.Add(new Thread(() => ThreadReader(boxEventRepositories[idx % numberOfRepositories], currentList, threadId, GetShardsPart(idx, count))));
            }

            var watch = Stopwatch.StartNew();
            watch.Start();
            for (var i = 0; i < count; ++i)
            {
                writeThreads[i].Start();
                readThreads[i].Start();
            }

            for (var i = 0; i < count; ++i)
                writeThreads[i].Join();

            var elapsed = watch.Elapsed;
            Console.WriteLine(elapsed.TotalMilliseconds);
            totalWrittenEvents = writtenEventsByThread.Sum(x => x.Count);
            for (var i = 0; i < count; ++i)
                readThreads[i].Join();
            watch.Stop();
            Console.WriteLine(watch.ElapsedMilliseconds);

            var actualBoxEvents = globalEventRepository.GetEvents(null, GetAllShards()).ToArray();
            LogEventBatch("allEvents", actualBoxEvents);
            CheckEqualEvents(writtenEventsByThread, actualBoxEvents);
            for (var i = 0; i < count; i++)
            {
                var readEvents = readEventsByThread[i];
                var writtenEvents = writtenEventsByThread[i];
                CheckEqualEvents(writtenEvents.ToArray(), readEvents.ToArray());
            }

            foreach (var eventRepository in boxEventRepositories)
                eventRepository.Dispose();
        }

        private const int shardsCount = 64;

        private void ThreadWriter(IEventRepository repository, List<Event> result, int threadId, string[] shards)
        {
            try
            {
                for (var i = 0; i < 1000; ++i)
                {
                    string scopeId;
                    do
                    {
                        scopeId = Guid.NewGuid().ToString();
                    } while (!shards.Contains(CalculateShard(scopeId)));

                    var eventContent = GenerateEventContent();
                    var eventInfo = repository.AddEvent(scopeId, eventContent);
                    Assert.AreEqual(scopeId, eventInfo.Id.ScopeId);
                    result.Add(new Event
                        {
                            EventContent = eventContent,
                            EventInfo = eventInfo,
                        });
                    LogEventBatch("ThreadWriter" + threadId.ToString("D2"), new[] {eventInfo});
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Throws exception " + e.GetType().Name + ". " + e.Message + Environment.NewLine + e.StackTrace);
                throw;
            }
        }

        private void ThreadReader(IEventRepository repository, SortedSet<Event> result, int threadId, string[] shards)
        {
            var sw = Stopwatch.StartNew();
            try
            {
                EventInfo exclusiveStartEventInfo = null;
                while (true)
                {
                    var readEvents = repository.GetEventsWithUnstableZone(exclusiveStartEventInfo, shards).ToList();
                    var stableZone = readEvents.TakeWhile(x => x.StableZone).ToList();
                    exclusiveStartEventInfo = stableZone.Select(x => x.Event.EventInfo).LastOrDefault() ?? exclusiveStartEventInfo;
                    foreach (var readEvent in stableZone)
                        result.Add(readEvent.Event);

                    var exclusiveId = exclusiveStartEventInfo == null ? "null" : exclusiveStartEventInfo.Id.ScopeId + "_" + exclusiveStartEventInfo.Id.Id;
                    LogEventBatch("ThreadReader" + threadId.ToString("D2"), stableZone.Select(x => x.Event).ToArray(), "readEventsFrom: " + exclusiveId);

                    if (result.Count > 1000)
                        throw new Exception($"BUG {result.Count} > 1000");
                    if (totalWrittenEvents != 0 && result.Count == 1000)
                        return;
                    Thread.Sleep(ThreadLocalRandom.Instance.Next(100));
                    if (sw.ElapsedMilliseconds > 480000)
                        throw new Exception($"Expected {totalWrittenEvents} but was {result.Count}");
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ThreadReader #" + threadId + " failed. Throws exception " + e.GetType().Name + ". " + e.Message + Environment.NewLine + e.StackTrace);
                throw;
            }
        }

        private static string[] GetShardsPart(int idx, int count)
        {
            var len = shardsCount / count;
            var l = len * idx;
            var r = idx == count - 1 ? shardsCount : len * (idx + 1);
            var list = new List<string>();
            for (var i = l; i < r; i++)
                list.Add(i.ToString());
            return list.ToArray();
        }

        private IEventRepository CreateBoxEventRepository()
        {
            return CreateBoxEventRepository(CalculateShard);
        }

        private string CalculateShard(string scopeId)
        {
            return keyDistributor.Distribute(scopeId).ToString();
        }

        private string CalculateShard(EventId eventId, object eventContent)
        {
            return keyDistributor.Distribute(eventId.ScopeId).ToString();
        }

        private static string[] GetAllShards()
        {
            return new string[shardsCount].Select((x, idx) => GetShardByIndex(idx)).ToArray();
        }

        private static string GetShardByIndex(int idx)
        {
            return idx.ToString();
        }

        private volatile int totalWrittenEvents;

        private IEventRepository globalEventRepository;
        private KeyDistributor keyDistributor;
    }
}