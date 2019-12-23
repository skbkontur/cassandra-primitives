using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;

using NUnit.Framework;

using SkbKontur.Cassandra.Primitives.EventLog;
using SkbKontur.Cassandra.Primitives.EventLog.Primitives;
using SkbKontur.Cassandra.Primitives.EventLog.Sharding;
using SkbKontur.Cassandra.TimeBasedUuid;

namespace CassandraPrimitives.Tests.FunctionalTests.Tests.EventRepositoryTests
{
    public abstract class BoxEventRepositoryMultiThreadTestBase : BoxEventRepositoryTestBase
    {
        public override void SetUp()
        {
            base.SetUp();
            globalEventRepository = CreateBoxEventRepository();
        }

        public override void TearDown()
        {
            globalEventRepository.Dispose();
            base.TearDown();
        }

        protected void DoTestMultiple()
        {
            for (var i = 0; i < 100; ++i)
            {
                TearDown();
                SetUp();
                Console.WriteLine("Run {0}. Time {1:dd.MM.yyyy HH:mm:ss}", i, Timestamp.Now.ToDateTime());
                DoTestMultiThread();
            }
        }

        protected void DoTestMultiThread()
        {
            totalWrittenEvents = 0;
            const int count = 5;

            var writeThreads = new List<Thread>();
            var writtenEventsByThread = new List<Event>[count];
            for (var i = 0; i < count; ++i)
            {
                var currentList = writtenEventsByThread[i] = new List<Event>();
                var threadId = i;
                writeThreads.Add(new Thread(() => ThreadWriter(globalEventRepository, currentList, threadId)));
            }

            var readThreads = new List<Thread>();
            var readEventsByThread = new SortedSet<Event>[count];
            for (var i = 0; i < count; ++i)
            {
                var currentSet = readEventsByThread[i] = new SortedSet<Event>(new EventComparer());
                var threadId = i;
                readThreads.Add(new Thread(() => ThreadReader(globalEventRepository, currentSet, threadId)));
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
            foreach (var readEvents in readEventsByThread)
                CheckEqualEvents(actualBoxEvents, readEvents.ToArray());
        }

        protected void DoTestMultipleRepositoriesMultiThread()
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
                var j = i;
                var threadId = i;
                writeThreads.Add(new Thread(() => ThreadWriter(boxEventRepositories[j % numberOfRepositories], currentList, threadId)));
            }

            var readThreads = new List<Thread>();
            var readEventsByThread = new SortedSet<Event>[count];
            for (var i = 0; i < count; ++i)
            {
                var currentList = readEventsByThread[i] = new SortedSet<Event>(new EventComparer());
                var j = i;
                var threadId = i;
                readThreads.Add(new Thread(() => ThreadReader(boxEventRepositories[j % numberOfRepositories], currentList, threadId)));
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
            foreach (var readEvents in readEventsByThread)
                CheckEqualEvents(actualBoxEvents, readEvents.ToArray());

            foreach (var eventRepository in boxEventRepositories)
                eventRepository.Dispose();
        }

        protected abstract int ShardsCount { get; }

        private void ThreadWriter(IEventRepository repository, List<Event> result, int threadId)
        {
            try
            {
                var totalWrites = 0;
                long totalMilliseconds = 0;
                long maxMilliseconds = 0;
                long bigger500ms = 0;
                long bigger1000ms = 0;
                for (var i = 0; i < 1000; ++i)
                {
                    var scopeId = Guid.NewGuid().ToString();
                    var eventContent = GenerateEventContent();
                    var stopWatch = Stopwatch.StartNew();
                    var eventInfo = repository.AddEvent(scopeId, eventContent);
                    totalWrites++;
                    var elapsedMilliseconds = stopWatch.ElapsedMilliseconds;
                    totalMilliseconds += elapsedMilliseconds;
                    if (elapsedMilliseconds > maxMilliseconds)
                        maxMilliseconds = elapsedMilliseconds;
                    if (elapsedMilliseconds > 500)
                        bigger500ms++;
                    if (elapsedMilliseconds > 1000)
                        bigger1000ms++;
                    Assert.AreEqual(scopeId, eventInfo.Id.ScopeId);
                    result.Add(new Event
                        {
                            EventContent = eventContent,
                            EventInfo = eventInfo,
                        });
                    LogEventBatch("ThreadWriter" + threadId.ToString("D2"), new[] {eventInfo});
                }
                Console.WriteLine("ThreadWriter {0} finished. " +
                                  "TotalWrites = {1}, " +
                                  "TotalMilliseconds = {2}, " +
                                  "Average = {3}, " +
                                  "MaxMilliseconds = {4}, " +
                                  "Bigger500ms = {5}, " +
                                  "Bigger1000ms = {6}",
                                  threadId.ToString("D2"),
                                  totalWrites,
                                  totalMilliseconds,
                                  (double)totalMilliseconds / Math.Max(totalWrites, 1),
                                  maxMilliseconds,
                                  bigger500ms,
                                  bigger1000ms);
            }
            catch (Exception e)
            {
                Console.WriteLine("Throws exception " + e.GetType().Name + ". " + e.Message + Environment.NewLine + e.StackTrace);
                throw;
            }
        }

        private void ThreadReader(IEventRepository repository, SortedSet<Event> result, int threadId)
        {
            var sw = Stopwatch.StartNew();
            try
            {
                EventInfo exclusiveEventInfo = null;
                while (true)
                {
                    var readEvents = repository.GetEventsWithUnstableZone(exclusiveEventInfo, GetAllShards()).ToList();
                    var stableZone = readEvents.TakeWhile(x => x.StableZone).ToList();
                    exclusiveEventInfo = stableZone.Select(x => x.Event.EventInfo).LastOrDefault() ?? exclusiveEventInfo;
                    foreach (var readEvent in stableZone)
                        result.Add(readEvent.Event);

                    var exclusiveId = exclusiveEventInfo == null ? "null" : exclusiveEventInfo.Id.ScopeId + "_" + exclusiveEventInfo.Id.Id;
                    LogEventBatch("ThreadReader" + threadId.ToString("D2"), stableZone.Select(x => x.Event).ToArray(), "readEventsFrom: " + exclusiveId);

                    if (totalWrittenEvents != 0 && result.Count > totalWrittenEvents)
                        throw new Exception($"BUG {result.Count} > {totalWrittenEvents}");
                    if (totalWrittenEvents != 0 && result.Count == totalWrittenEvents)
                    {
                        Console.WriteLine("ThreadReader" + threadId.ToString("D2") + " finished.");
                        return;
                    }
                    Thread.Sleep(ThreadLocalRandom.Instance.Next(100));
                    if (sw.ElapsedMilliseconds > 180000)
                        throw new Exception($"Expected {totalWrittenEvents} but was {result.Count}");
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ThreadReader #" + threadId + " failed. Throws exception " + e.GetType().Name + ". " + e.Message + Environment.NewLine + e.StackTrace);
                throw;
            }
        }

        private IEventRepository CreateBoxEventRepository()
        {
            return CreateBoxEventRepository(CalculateShard);
        }

        private string CalculateShard(EventId eventId, object eventContent)
        {
            var keyDistributor = new KeyDistributor(ShardsCount);
            return keyDistributor.Distribute(eventId.ScopeId).ToString();
        }

        private string[] GetAllShards()
        {
            return new string[ShardsCount].Select((x, idx) => GetShardByIndex(idx)).ToArray();
        }

        private string GetShardByIndex(int idx)
        {
            return idx.ToString();
        }

        private volatile int totalWrittenEvents;

        private IEventRepository globalEventRepository;
    }
}