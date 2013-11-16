using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;

using NUnit.Framework;

using SKBKontur.Catalogue.CassandraPrimitives.EventLog.EventLog;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Primitives;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Sharding;

using log4net;

namespace SKBKontur.Catalogue.CassandraPrimitives.FunctionalTests.Tests.EventRepositoryTests
{
    public class SpeedTests : BoxEventRepositoryTestBase
    {
        public override void SetUp()
        {
            base.SetUp();
            eventRepositoryForWrite = CreateBoxEventRepository();
            eventRepositoryForRead = CreateBoxEventRepository();
        }

        public override void TearDown()
        {
            eventRepositoryForWrite.Dispose();
            eventRepositoryForRead.Dispose();
            base.TearDown();
        }

        [Test, Ignore]
        public void DoTestMultipleRepositoriesMultiThread()
        {
            totalWrittenEvents = 0;
            const int writerThreadsCount = 1;

            var writeThreads = new List<Thread>();
            var writtenEventsByThread = new List<Event>[writerThreadsCount];
            for(var i = 0; i < writerThreadsCount; ++i)
            {
                var currentList = writtenEventsByThread[i] = new List<Event>();
                var threadId = i;
                writeThreads.Add(new Thread(() => ThreadWriter(eventRepositoryForWrite, currentList, threadId)));
            }

            const int readerThreadsCount = 1;
            var readThreads = new List<Thread>();
            var readEventsByThread = new SortedSet<Event>[readerThreadsCount];
            for(var i = 0; i < readerThreadsCount; ++i)
            {
                var currentList = readEventsByThread[i] = new SortedSet<Event>(new EventComparer());
                var j = i;
                var threadId = i;
                readThreads.Add(new Thread(() => ThreadReader(eventRepositoryForRead, currentList, threadId)));
            }

            var watch = Stopwatch.StartNew();
            watch.Start();
            for(var i = 0; i < writerThreadsCount; ++i)
                writeThreads[i].Start();
            for(var i = 0; i < readerThreadsCount; ++i)
                readThreads[i].Start();

            for(var i = 0; i < writerThreadsCount; ++i)
                writeThreads[i].Join();

            var elapsed = watch.Elapsed;
            Console.WriteLine(elapsed.TotalMilliseconds);

            totalWrittenEvents = writtenEventsByThread.Sum(x => x.Count);
            for(var i = 0; i < writerThreadsCount; ++i)
                readThreads[i].Join();
            watch.Stop();
            Console.WriteLine(watch.ElapsedMilliseconds);
        }

        private void ThreadWriter(IEventRepository repository, List<Event> result, int threadId)
        {
            try
            {
                for(var i = 0; i < 100000; ++i)
                {
                    var scopeId = Guid.NewGuid().ToString();
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
                Console.WriteLine("ThreadWriter" + threadId.ToString("D2") + " finished.");
            }
            catch(Exception e)
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
                var random = new Random(Guid.NewGuid().GetHashCode());
                EventInfo exclusiveEventInfo = null;
                while(true)
                {
                    var sw1 = Stopwatch.StartNew();
                    var readEvents = repository.GetEventsWithUnstableZone(exclusiveEventInfo, GetAllShards()).ToList();
                    logger.InfoFormat("reader: {0} events for {1} ms", readEvents.Count, sw1.ElapsedMilliseconds);
                    sw1.Stop();

                    foreach(var readEvent in readEvents)
                    {
                        if(readEvent.StableZone)
                            exclusiveEventInfo = readEvent.Event.EventInfo;
                        result.Add(readEvent.Event);
                    }

                    var exclusiveId = exclusiveEventInfo == null ? "null" : exclusiveEventInfo.Id.ScopeId + "_" + exclusiveEventInfo.Id.Id;
                    LogEventBatch("ThreadReader" + threadId.ToString("D2"), readEvents.Select(x => x.Event).ToArray(), "readEventsFrom: " + exclusiveId);

                    if(totalWrittenEvents != 0 && result.Count > totalWrittenEvents)
                        throw new Exception(string.Format("BUG {0} > {1}", result.Count, totalWrittenEvents));
                    if(totalWrittenEvents != 0 && result.Count == totalWrittenEvents)
                    {
                        Console.WriteLine("ThreadReader" + threadId.ToString("D2") + " finished.");
                        return;
                    }
                    Thread.Sleep(random.Next(100));
                    if(sw.ElapsedMilliseconds > 180000)
                        throw new Exception(string.Format("Expected {0} but was {1}", totalWrittenEvents, result.Count));
                }
            }
            catch(Exception e)
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

        private volatile int totalWrittenEvents;
        private readonly ILog logger = LogManager.GetLogger(typeof(SpeedTests));
        private IEventRepository eventRepositoryForWrite;
        private IEventRepository eventRepositoryForRead;
        private const int shardsCount = 1;
    }
}