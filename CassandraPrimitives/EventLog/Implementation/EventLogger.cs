using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

using GroBuf;

using MoreLinq;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Connections;

using SkbKontur.Cassandra.TimeBasedUuid;

using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Exceptions;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Linq;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Primitives;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Profiling;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.Primitives;

using Vostok.Logging.Abstractions;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.Implementation
{
    internal class EventLogger : IEventLogger
    {
        public EventLogger(
            ICassandraCluster cassandraCluster,
            ISerializer serializer,
            ColumnFamilyFullName eventLogColumnFamily,
            IEventLogPointerCreator eventLogPointerCreator,
            Func<IQueueRaker> createQueueRaker,
            IEventLoggerAdditionalInfoRepository eventLoggerAdditionalInfoRepository,
            IEventLogProfiler profiler,
            ILog logger)
        {
            this.serializer = serializer;
            this.eventLogPointerCreator = eventLogPointerCreator;
            this.createQueueRaker = createQueueRaker;
            this.eventLoggerAdditionalInfoRepository = eventLoggerAdditionalInfoRepository;
            this.profiler = profiler;
            this.logger = logger;
            columnFamilyConnection = cassandraCluster.RetrieveColumnFamilyConnection(eventLogColumnFamily.KeyspaceName, eventLogColumnFamily.ColumnFamilyName);
        }

        public void Dispose()
        {
            wasDisposed = true;
            lock (lockObject)
            {
                if (wasInitialized)
                    queueRaker.Dispose();
            }
        }

        public async Task<EventInfo[]> WriteAsync(params EventStorageElement[] events)
        {
            InitializeOnce();
            var eventBatches = events.Batch(1000).Select(x => x.ToArray());
            var eventInfos = new List<EventInfo>();
            foreach (var eventBatch in eventBatches)
            {
                //todo maybe in parallel ??
                var writeBatch = await WriteBatchAsync(eventBatch).ConfigureAwait(false);
                eventInfos.AddRange(writeBatch);
            }

            var dict = eventInfos.ToDictionary(x => x.Id);
            var result = events.Select(x => dict[x.EventInfo.Id]).ToArray();
            return result;
        }

        public EventInfo[] Write(params EventStorageElement[] events)
        {
            return WriteAsync(events).Result;
        }

        public IEnumerable<EventStorageElementContainer> ReadEventsWithUnstableZone(EventInfo startEventInfo, string[] shards, out EventInfo newExclusiveEventInfo)
        {
            InitializeOnce();
            if (shards == null || shards.Length == 0)
                throw new ShardsArrayIsEmptyException();

            if (startEventInfo == null)
                startEventInfo = eventLoggerAdditionalInfoRepository.GetFirstEventInfo();

            var finishEventInfo = eventLoggerAdditionalInfoRepository.GetGoodLastEventInfo();
            if (finishEventInfo == null)
            {
                logger.Info("FinishEventPointer not found");
                newExclusiveEventInfo = startEventInfo;
                return new EventStorageElementContainer[0];
            }
            newExclusiveEventInfo = finishEventInfo;
            return ReadEventsWithUnstableZone(startEventInfo, finishEventInfo, shards);
        }

        private IEnumerable<EventStorageElementContainer> ReadEventsWithUnstableZone(EventInfo startEventInfo, EventInfo finishEventInfo, string[] shards)
        {
            var eventRecords = InnerReadEvents(startEventInfo, finishEventInfo, shards, 1000);
            var stableZone = true;
            foreach (var eventLogRecord in eventRecords)
            {
                if (eventLogRecord.StorageElement.EventInfo.Ticks > finishEventInfo.Ticks)
                    yield break;

                if (eventLogRecord.IsBad)
                    stableZone = false;

                yield return new EventStorageElementContainer
                    {
                        EventStorageElement = eventLogRecord.StorageElement,
                        StableZone = stableZone
                    };
            }
        }

        private void InitializeOnce()
        {
            if (!wasInitialized)
            {
                lock (lockObject)
                {
                    if (wasDisposed)
                        throw new EventLoggerWasDisposedException();
                    if (!wasInitialized)
                    {
                        eventLoggerAdditionalInfoRepository.GetOrCreateFirstEventInfo(new EventInfo
                            {
                                Shard = "Null",
                                Ticks = Timestamp.Now.Ticks,
                                Id = new EventId
                                    {
                                        ScopeId = Guid.NewGuid().ToString(),
                                        Id = Guid.NewGuid().ToString()
                                    }
                            });
                        queueRaker = createQueueRaker();
                        wasInitialized = true;
                    }
                }
            }
        }

        private async Task<EventInfo[]> WriteBatchAsync(EventStorageElement[] eventBatch)
        {
            var dict = eventBatch.ToDictionary(x => x.EventInfo.Id);
            var result = new List<EventInfo>();

            var batchForWrite = eventBatch;
            var stopwatch = Stopwatch.StartNew();
            var totalAttemptCount = 0;
            var timeOfSleep = TimeSpan.FromTicks(0);
            try
            {
                for (var attempt = 0; !wasDisposed && attempt < attemptCount; ++attempt)
                {
                    totalAttemptCount++;
                    var enqueueResult = await queueRaker.ProcessAsync(batchForWrite, attempt).ConfigureAwait(false);
                    batchForWrite = enqueueResult.failureIds.Select(x => dict[x]).ToArray();
                    result.AddRange(enqueueResult.successInfos);
                    if (batchForWrite.Length == 0) return result.ToArray();
                    var sleepTime = ThreadLocalRandom.Instance.Next(10, 20);
                    var sleepStopwatch = Stopwatch.StartNew();
                    try
                    {
                        await Task.Delay(sleepTime).ConfigureAwait(false);
                    }
                    finally
                    {
                        timeOfSleep += sleepStopwatch.Elapsed;
                    }
                    if (attempt > 1)
                        logger.Warn("Big attempt: attempt = {0}, sleepTime = {1}", attempt, sleepTime);
                }
            }
            finally
            {
                profiler.AfterWriteBatch(stopwatch.Elapsed, eventBatch.Length, totalAttemptCount, timeOfSleep);
            }

            if (wasDisposed)
                throw new CouldNotWriteBoxEventException("This instance of eventLogger was disposed");
            throw new CouldNotWriteBoxEventException($"Could not write in {attemptCount} attempts");
        }

        private IEnumerable<EventLogRecord> InnerReadEvents(EventInfo startEventInfo, EventInfo finishEventInfo, string[] shards, int batchCount)
        {
            var startRowNumber = eventLogPointerCreator.GetRowNumber(startEventInfo);
            var finishRowNumber = eventLogPointerCreator.GetRowNumber(finishEventInfo);
            var startEventPointer = eventLogPointerCreator.Create(startEventInfo);

            var getRowsBatchCount = batchCount / shards.Length;
            var rowKeys = shards.Select(shard => eventLogPointerCreator.ChangeShard(startRowNumber, shard)).ToArray();
            var rows1 = columnFamilyConnection.GetRowsExclusive(rowKeys, startEventPointer.ColumnName, getRowsBatchCount).ToDictionary(x => x.Key, x => x.Value);
            var rows2 = new Dictionary<string, Column[]>();
            var needColumns2 = false;
            if (startRowNumber < finishRowNumber && rows1.Count(x => x.Value.Length < getRowsBatchCount) > 2) //todo подумать над константой
            {
                var rowKeys2 = shards.Select(shard => eventLogPointerCreator.ChangeShard(startRowNumber + 1, shard)).ToArray();
                rows2 = columnFamilyConnection.GetRowsExclusive(rowKeys2, null, getRowsBatchCount).ToDictionary(x => x.Key, x => x.Value);
                needColumns2 = true;
            }

            var list = new List<IEnumerable<EventLogRecord>>();
            foreach (var shard in shards)
            {
                var rowKey1 = eventLogPointerCreator.ChangeShard(startRowNumber, shard);
                var rowKey2 = eventLogPointerCreator.ChangeShard(startRowNumber + 1, shard);
                var columns1 = GetColumnsFromDict(rows1, rowKey1);
                var columns2 = GetColumnsFromDict(rows2, rowKey2);
                var columns = needColumns2 ? new[] {columns1, columns2} : new[] {columns1};
                var eventLogRecords = GetEventsEnumerable(shard, startRowNumber, startEventPointer.ColumnName, finishRowNumber, columns, batchCount, getRowsBatchCount);
                list.Add(eventLogRecords);
            }
            IEnumerable<EventLogRecord> res = new EventLogRecord[0];

            res = list.Aggregate(res, (current, x) => current.SortedMerge(x, (a, b) => a.StorageElement.EventInfo.CompareTo(b.StorageElement.EventInfo)));
            return res;
        }

        private static Column[] GetColumnsFromDict(Dictionary<string, Column[]> rows, string rowKey)
        {
            if (!rows.TryGetValue(rowKey, out var columns) || columns == null)
                columns = new Column[0];
            return columns;
        }

        private IEnumerable<EventLogRecord> GetEventsEnumerable(string shard, long startRowNumber, string startEventColumnName, long finishRowNumber, Column[][] columns, int batchCount, int getRowsBatchCount)
        {
            for (var i = startRowNumber; i <= finishRowNumber; i++)
            {
                string currentExclusiveColumn = null;

                if (i - startRowNumber < columns.Length)
                {
                    var cols = columns[i - startRowNumber];
                    currentExclusiveColumn = cols.Length > 0 ? cols.Last().Name : (i == startRowNumber ? startEventColumnName : null);

                    foreach (var column in cols)
                        yield return GetEventLogRecord(column);
                    if (cols.Length < getRowsBatchCount)
                        continue;
                }

                var currentRowKey = eventLogPointerCreator.ChangeShard(i, shard);
                while (true)
                {
                    var cols = columnFamilyConnection.GetColumns(currentRowKey, currentExclusiveColumn, batchCount);
                    if (cols.Length == 0) break;
                    foreach (var column in cols)
                        yield return GetEventLogRecord(column);
                    currentExclusiveColumn = cols.Last().Name;
                }
            }
        }

        private EventLogRecord GetEventLogRecord(Column column)
        {
            var columnValue = column.Value;
            return serializer.Deserialize<EventLogRecord>(columnValue);
        }

        private const int attemptCount = 20;

        private readonly ILog logger;
        private readonly ISerializer serializer;
        private readonly IEventLogPointerCreator eventLogPointerCreator;
        private readonly Func<IQueueRaker> createQueueRaker;
        private readonly IEventLoggerAdditionalInfoRepository eventLoggerAdditionalInfoRepository;
        private readonly IEventLogProfiler profiler;
        private readonly IColumnFamilyConnection columnFamilyConnection;

        private volatile IQueueRaker queueRaker;
        private volatile bool wasDisposed;
        private volatile bool wasInitialized;
        private readonly object lockObject = new object();
    }
}