using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

using MoreLinq;

using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Exceptions;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Primitives;

using log4net;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.EventLog
{
    public class QueueRaker : IQueueRaker
    {
        public QueueRaker(
            IEventStorage eventStorage,
            IEventLoggerAdditionalInfoRepository eventLoggerAdditionalInfoRepository,
            IEventInfoRepository eventInfoRepository)
        {
            this.eventStorage = eventStorage;
            this.eventLoggerAdditionalInfoRepository = eventLoggerAdditionalInfoRepository;
            this.eventInfoRepository = eventInfoRepository;
            manualResetEventPool = new ManualResetEventPool();
            queue = new Queue<QueueEntry>();
            Start();
        }

        public void Dispose()
        {
            if(wasDisposed)
                return;
            wasDisposed = true;
            @event.Set();
            thread.Join();
            @event.Dispose();

            lock(lockObject)
            {
                var batch = queue.ToList();
                foreach(var entry in batch)
                {
                    entry.result.successInfos = new EventInfo[0];
                    entry.result.failureIds = entry.events.Select(x => x.EventInfo.Id).ToArray();
                    entry.result.Signal();
                }
                queue.Clear();
            }
            manualResetEventPool.Dispose();
        }

        public DeferredResult Enqueue(EventStorageElement[] events, int priority)
        {
            lock(lockObject)
            {
                if(wasDisposed)
                    throw new CouldNotWriteBoxEventException("This instance of eventLogger was disposed");
                var result = new DeferredResult(manualResetEventPool);
                queue.Enqueue(new QueueEntry
                    {
                        events = events,
                        result = result,
                        priority = priority
                    });
                if(queue.Count * runs >= sum && queue.Count >= 10)
                    @event.Set();
                return result;
            }
        }

        public void Start()
        {
            thread = new Thread(Rake);
            thread.Start();
        }

        private List<QueueEntry> GetBatchFromQueue()
        {
            lock(lockObject)
            {
                var result = queue.OrderBy(entry => entry.priority).ToList();
                queue.Clear();
                return result;
            }
        }

        private void Rake()
        {
            while(!wasDisposed)
            {
                @event.WaitOne(1);
                var batch = GetBatchFromQueue();
                @event.Reset();
                var batchCount = batch.Sum(x => x.events.Length);
                if(batchCount == 0)
                    continue;
                ++runs;
                sum += batchCount;
                var eventsBatch = batch.SelectMany(x => x.events).ToArray();

                try
                {
                    var nowTicks = Math.Max(eventLoggerAdditionalInfoRepository.GetGoodLastEventInfo().Ticks, DateTime.UtcNow.Ticks);

                    var index = 0;
                    foreach(var entry in batch)
                    {
                        foreach(var e in entry.events)
                        {
                            e.EventInfo.Ticks = nowTicks + index + entry.priority * 10;
                            index++;
                        }
                    }

                    // todo подумать, какой TTL взять
                    eventStorage.Write(eventsBatch.Select(x => new EventLogRecord {IsBad = true, StorageElement = x}).ToArray(), nowTicks, 60);

                    var lastGoodEventInfo = eventLoggerAdditionalInfoRepository.GetGoodLastEventInfo();

                    var lastEventInfoFromCurrentBatch = eventsBatch.MaxBy(x => x.EventInfo.Ticks).EventInfo;
                    if(lastEventInfoFromCurrentBatch.Ticks > lastGoodEventInfo.Ticks)
                        eventLoggerAdditionalInfoRepository.SetLastEventInfo(lastEventInfoFromCurrentBatch);

                    var badEvents = eventsBatch.Where(x => x.EventInfo.Ticks <= lastGoodEventInfo.Ticks).ToArray();
                    var goodEvents = eventsBatch.Where(x => x.EventInfo.Ticks > lastGoodEventInfo.Ticks).ToArray();

                    if(badEvents.Length > 0)
                        eventStorage.Delete(badEvents.Select(x => x.EventInfo).ToArray(), nowTicks + 1);

                    if(goodEvents.Length > 0)
                    {
                        SetEventMetas(goodEvents);
                        eventStorage.Write(goodEvents.Select(x => new EventLogRecord {IsBad = false, StorageElement = x}).ToArray(), nowTicks + 1);
                    }

                    foreach(var entry in batch)
                    {
                        entry.result.successInfos = entry.events.Where(x => x.EventInfo.Ticks > lastGoodEventInfo.Ticks).Select(x => x.EventInfo).ToArray();
                        entry.result.failureIds = entry.events.Where(x => x.EventInfo.Ticks <= lastGoodEventInfo.Ticks).Select(x => x.EventInfo.Id).ToArray();
                        entry.result.Signal();
                    }
                }
                catch(Exception e)
                {
                    logger.Error(e);
                    foreach(var entry in batch)
                    {
                        entry.result.successInfos = new EventInfo[0];
                        entry.result.failureIds = entry.events.Select(x => x.EventInfo.Id).ToArray();
                        entry.result.Signal();
                    }
                }
            }
        }

        private void SetEventMetas(EventStorageElement[] events)
        {
            var metas = events.Select(x => x.EventInfo).ToArray();
            eventInfoRepository.Write(metas, DateTime.UtcNow);
        }

        private volatile float sum;
        private volatile float runs;
        private readonly IEventStorage eventStorage;
        private readonly IEventLoggerAdditionalInfoRepository eventLoggerAdditionalInfoRepository;
        private readonly IEventInfoRepository eventInfoRepository;
        private readonly ManualResetEventPool manualResetEventPool;
        private readonly ManualResetEvent @event = new ManualResetEvent(false);
        private volatile bool wasDisposed;
        private readonly object lockObject = new object();
        private Thread thread;
        private readonly Queue<QueueEntry> queue;
        private readonly ILog logger = LogManager.GetLogger(typeof(QueueRaker));
    }
}