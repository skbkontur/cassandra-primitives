using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using SKBKontur.Catalogue.CassandraPrimitives.EventLog;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Primitives;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Sharding;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Tests.EventRepositoryTests
{
    public class BoxEventRepositoryConsistencyTest : BoxEventRepositoryTestBase
    {
        [Test]
        public void TestCanReadEventAfterWrite()
        {
            var exceptionEvent = new ManualResetEvent(false);
            const int count = 5;
            var threads = new Thread[count];
            for (var i = 0; i < count; i++)
            {
                var threadId = i;
                var eventRepositoryForWrite = CreateBoxEventRepository();
                var eventRepositoryForRead = CreateBoxEventRepository();
                threads[threadId] = new Thread(() => RunThread(eventRepositoryForWrite, eventRepositoryForRead, GetAllShards(), threadId, exceptionEvent));
                threads[threadId].Start();
            }
            stopped = false;
            exceptionEvent.WaitOne(TimeSpan.FromMinutes(1));
            stopped = true;
            foreach (var thread in threads)
            {
                try
                {
                    thread.Join();
                }
                catch (Exception e)
                {
                    joinException = e;
                }
            }
            if (threadException != null)
                throw threadException;
            if (joinException != null)
                throw joinException;
        }

        private IEventRepository CreateBoxEventRepository()
        {
            return CreateBoxEventRepository(CalculateShard);
        }

        private string CalculateShard(EventId eventId, object eventContent)
        {
            var keyDistributor = new KeyDistributor(shardsCount);
            return keyDistributor.Distribute(eventId.ScopeId).ToString();
        }

        private string[] GetAllShards()
        {
            return new string[shardsCount].Select((x, idx) => GetShardByIndex(idx)).ToArray();
        }

        private string GetShardByIndex(int idx)
        {
            return idx.ToString();
        }

        private void RunThread(IEventRepository eventRepositoryForWrite, IEventRepository eventRepositoryForRead, string[] shards, int threadId, ManualResetEvent exceptionEvent)
        {
            try
            {
                while (stopped)
                {
                }
                EventInfo lastEventInfo = null;
                var totalWrittenEvents = 0;
                var totalReadEvents = 1;
                while (!stopped)
                {
                    var bag = new ConcurrentBag<EventId>();
                    var actions = new int[12].Select<int, Action>(i => (() =>
                        {
                            try
                            {
                                bag.Add(eventRepositoryForWrite.AddEvent(Guid.NewGuid().ToString(), GenerateEventContent()).Id);
                            }
                            catch (Exception e)
                            {
                                threadException = e;
                            }
                        })).ToArray();
                    Parallel.Invoke(actions);

                    totalWrittenEvents += 12;
                    var allCurrentEvents = eventRepositoryForRead.GetEventsWithUnstableZone(lastEventInfo, shards).ToArray();
                    totalReadEvents += allCurrentEvents.Length;
                    lastEventInfo = allCurrentEvents
                                        .TakeWhile(container => container.StableZone)
                                        .Select(container => container.Event.EventInfo).LastOrDefault() ?? lastEventInfo;
                    foreach (var eventId in bag)
                    {
                        if (!allCurrentEvents.Any(container => container.Event.EventInfo.Id.Equals(eventId)))
                            throw new Exception($"Поток {threadId} записал эвент [ScopeId = {eventId.ScopeId}, EventId = {eventId.Id}], но не прочитал его");
                    }
                }
                Console.WriteLine("Thread # {0} statistics: write {1} events, read {2} events", threadId, totalWrittenEvents, totalReadEvents);
            }
            catch (Exception e)
            {
                threadException = e;
                exceptionEvent.Set();
            }
        }

        private const int shardsCount = 64;

        private volatile Exception threadException;
        private volatile Exception joinException;
        private volatile bool stopped;
    }
}