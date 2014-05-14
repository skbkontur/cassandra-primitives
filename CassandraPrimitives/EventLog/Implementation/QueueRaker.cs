﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;

using MoreLinq;

using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Exceptions;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Primitives;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Profiling;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Utils;

using log4net;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.Implementation
{
    internal class QueueRaker : IQueueRaker
    {
        public QueueRaker(
            IEventStorage eventStorage,
            IEventLoggerAdditionalInfoRepository eventLoggerAdditionalInfoRepository,
            IEventInfoRepository eventInfoRepository,
            IEventLogProfiler profiler)
        {
            this.eventStorage = eventStorage;
            this.eventLoggerAdditionalInfoRepository = eventLoggerAdditionalInfoRepository;
            this.eventInfoRepository = eventInfoRepository;
            this.profiler = profiler;
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
                var result = new DeferredResult(manualResetEventPool, profiler);
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
                var stopwatch = Stopwatch.StartNew();
                @event.WaitOne(1);
                var batch = GetBatchFromQueue();
                @event.Reset();
                var batchCount = batch.Sum(x => x.events.Length);
                if(batchCount == 0)
                    continue;

                totalCount++;
                totalWaitTime += stopwatch.Elapsed;
                totalEventCount += batchCount;
                totalEventBatchCount += batch.Count;
                profiler.BeforeRake(stopwatch.Elapsed, totalEventCount, totalEventBatchCount, batch.Select(x => x.SinceCreateElapsed).ToArray());
                if(DateTime.Now - outputDateTime > TimeSpan.FromMinutes(1))
                {
                    logger.Info(GetRakeStatistics());
                    outputDateTime = DateTime.Now;
                    totalEventCount = 0;
                    totalEventBatchCount = 0;
                    totalWaitTime = TimeSpan.FromMilliseconds(0);
                    totalCount = 0;
                }

                ++runs;
                sum += batchCount;
                var eventsBatch = batch.SelectMany(x => x.events).ToArray();

                try
                {
                    var writeStopwatch = Stopwatch.StartNew();
                    var getGoodLastEventInfo1Stopwatch = Stopwatch.StartNew();
                    var nowTicks = Math.Max(eventLoggerAdditionalInfoRepository.GetGoodLastEventInfo().Ticks, DateTime.UtcNow.Ticks);
                    getGoodLastEventInfo1Stopwatch.Stop();

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
                    var writeEventsStopwatch = Stopwatch.StartNew();
                    eventStorage.Write(eventsBatch.Select(x => new EventLogRecord {IsBad = true, StorageElement = x}).ToArray(), nowTicks, 60);
                    writeEventsStopwatch.Stop();

                    var getGoodLastEventInfo2Stopwatch = Stopwatch.StartNew();
                    var lastGoodEventInfo = eventLoggerAdditionalInfoRepository.GetGoodLastEventInfo();
                    getGoodLastEventInfo2Stopwatch.Stop();

                    var badEvents = eventsBatch.Where(x => x.EventInfo.Ticks <= lastGoodEventInfo.Ticks).ToArray();
                    var goodEvents = eventsBatch.Where(x => x.EventInfo.Ticks > lastGoodEventInfo.Ticks).ToArray();

                    var deleteBadEventsStopwatch = Stopwatch.StartNew();
                    if(badEvents.Length > 0)
                        eventStorage.Delete(badEvents.Select(x => x.EventInfo).ToArray(), nowTicks + 1);
                    deleteBadEventsStopwatch.Stop();

                    Stopwatch setEventMetasStopwatch = null;
                    Stopwatch setLastEventInfoStopwatch = null;
                    Stopwatch setEventsGoodStopwatch = null;
                    if(goodEvents.Length > 0)
                    {
                        setEventMetasStopwatch = Stopwatch.StartNew();
                        SetEventMetas(goodEvents);
                        setEventMetasStopwatch.Stop();

                        var lastEventInfoFromCurrentBatch = eventsBatch.MaxBy(x => x.EventInfo.Ticks).EventInfo;

                        setLastEventInfoStopwatch = Stopwatch.StartNew();
                        if(lastEventInfoFromCurrentBatch.Ticks > lastGoodEventInfo.Ticks)
                            eventLoggerAdditionalInfoRepository.SetLastEventInfo(lastEventInfoFromCurrentBatch);
                        setLastEventInfoStopwatch.Stop();

                        setEventsGoodStopwatch = Stopwatch.StartNew();
                        eventStorage.Write(goodEvents.Select(x => new EventLogRecord {IsBad = false, StorageElement = x}).ToArray(), nowTicks + 1);
                        setEventsGoodStopwatch.Stop();
                    }

                    foreach(var entry in batch)
                    {
                        entry.result.successInfos = entry.events.Where(x => x.EventInfo.Ticks > lastGoodEventInfo.Ticks).Select(x => x.EventInfo).ToArray();
                        entry.result.failureIds = entry.events.Where(x => x.EventInfo.Ticks <= lastGoodEventInfo.Ticks).Select(x => x.EventInfo.Id).ToArray();
                        entry.result.Signal();
                    }

                    profiler.AfterRake(
                        GetElapsed(writeStopwatch),
                        GetElapsed(getGoodLastEventInfo1Stopwatch),
                        GetElapsed(getGoodLastEventInfo2Stopwatch),
                        GetElapsed(writeEventsStopwatch),
                        GetElapsed(deleteBadEventsStopwatch),
                        GetElapsed(setEventMetasStopwatch),
                        GetElapsed(setLastEventInfoStopwatch),
                        GetElapsed(setEventsGoodStopwatch));
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

        private TimeSpan GetElapsed(Stopwatch stopwatch)
        {
            return stopwatch == null ? TimeSpan.FromTicks(0) : stopwatch.Elapsed;
        }

        private string GetRakeStatistics()
        {
            var result = new StringBuilder();
            result.AppendLine("Raker statistics: ");
            result.AppendFormat("  Average event count: {0}" + Environment.NewLine, (double)totalEventCount / (totalCount + 1));
            result.AppendFormat("  Average event batch count: {0}" + Environment.NewLine, (double)totalEventBatchCount / (totalCount + 1));
            result.AppendFormat("  Rakes count: {0}" + Environment.NewLine, (double)totalCount);
            result.AppendFormat("  Rake waits: {0}" + Environment.NewLine, totalWaitTime.TotalMilliseconds / (totalCount + 1));
            return result.ToString();
        }

        private void SetEventMetas(EventStorageElement[] events)
        {
            var metas = events.Select(x => x.EventInfo).ToArray();
            eventInfoRepository.Write(metas, DateTime.UtcNow);
        }

        private DateTime outputDateTime = DateTime.Now;
        private long totalEventCount;
        private long totalEventBatchCount;
        private TimeSpan totalWaitTime = TimeSpan.FromMilliseconds(0);
        private long totalCount;

        private volatile float sum;
        private volatile float runs;
        private readonly IEventStorage eventStorage;
        private readonly IEventLoggerAdditionalInfoRepository eventLoggerAdditionalInfoRepository;
        private readonly IEventInfoRepository eventInfoRepository;
        private readonly IEventLogProfiler profiler;
        private readonly ManualResetEventPool manualResetEventPool;
        private readonly ManualResetEvent @event = new ManualResetEvent(false);
        private volatile bool wasDisposed;
        private readonly object lockObject = new object();
        private Thread thread;
        private readonly Queue<QueueEntry> queue;
        private readonly ILog logger = LogManager.GetLogger(typeof(QueueRaker));
    }
}