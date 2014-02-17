using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

using GroBuf;

using MoreLinq;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Connections;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Exceptions;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Linq;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Primitives;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.GlobalTicksHolder;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.Primitives;

using log4net;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.EventLog
{
    public class EventLogger : IEventLogger
    {
        public EventLogger(
            ICassandraCluster cassandraCluster,
            ISerializer serializer,
            ColumnFamilyFullName eventLogColumnFamily,
            IEventInfoRepository eventInfoRepository,
            IEventLogPointerCreator eventLogPointerCreator,
            Func<IQueueRaker> createQueueRaker,
            IEventLoggerAdditionalInfoRepository eventLoggerAdditionalInfoRepository,
            IGlobalTime globalTime)
        {
            this.serializer = serializer;
            this.eventInfoRepository = eventInfoRepository;
            this.eventLogPointerCreator = eventLogPointerCreator;
            this.createQueueRaker = createQueueRaker;
            this.eventLoggerAdditionalInfoRepository = eventLoggerAdditionalInfoRepository;
            this.globalTime = globalTime;
            columnFamilyConnection = cassandraCluster.RetrieveColumnFamilyConnection(eventLogColumnFamily.KeyspaceName, eventLogColumnFamily.ColumnFamilyName);
        }

        public void Dispose()
        {
            wasDisposed = true;
            lock(lockObject)
            {
                if(wasInitialized)
                    queueRaker.Dispose();
            }
        }

        public EventStorageElement ReadEvent(EventId eventId)
        {
            InitializeOnce();
            var eventInfo = eventInfoRepository.TryRead(eventId);
            var eventPointer = eventLogPointerCreator.Create(eventInfo);
            var column = columnFamilyConnection.GetColumn(eventPointer.RowKey, eventPointer.ColumnName);
            return serializer.Deserialize<EventLogRecord>(column.Value).StorageElement;
        }

        public IEnumerable<EventStorageElementContainer> ReadEventsWithUnstableZone(EventInfo startEventInfo, string[] shards, out EventInfo newExclusiveEventInfo)
        {
            InitializeOnce();
            if(shards == null || shards.Length == 0)
                throw new ShardsArrayIsEmptyException();

            if(startEventInfo == null)
                startEventInfo = eventLoggerAdditionalInfoRepository.GetFirstEventInfo();

            var finishEventInfo = eventLoggerAdditionalInfoRepository.GetGoodLastEventInfo();
            if(finishEventInfo == null)
            {
                logger.InfoFormat("FinishEventPointer not found");
                newExclusiveEventInfo = startEventInfo;
                return new EventStorageElementContainer[0];
            }
            newExclusiveEventInfo = finishEventInfo;
            return ReadEventsWithUnstableZone(startEventInfo, finishEventInfo, shards);
        }

        public EventInfo[] Write(params EventStorageElement[] events)
        {
            InitializeOnce();
            var eventBatches = events.Batch(1000).Select(x => x.ToArray());
            var eventInfos = new List<EventInfo>();
            foreach(var eventBatch in eventBatches)
                eventInfos.AddRange(WriteBatch(eventBatch));

            var dict = eventInfos.ToDictionary(x => x.Id);
            var result = events.Select(x => dict[x.EventInfo.Id]).ToArray();
            return result;
        }

        private IEnumerable<EventStorageElementContainer> ReadEventsWithUnstableZone(EventInfo startEventInfo, EventInfo finishEventInfo, string[] shards)
        {
            var eventRecords = InnerReadEvents(startEventInfo, finishEventInfo, shards, 1000);
            var stableZone = true;
            foreach(var eventLogRecord in eventRecords)
            {
                if(eventLogRecord.StorageElement.EventInfo.Ticks > finishEventInfo.Ticks)
                    yield break;

                if(eventLogRecord.IsBad)
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
            if(!wasInitialized)
            {
                lock(lockObject)
                {
                    if(wasDisposed)
                        throw new EventLoggerWasDisposedException();
                    if(!wasInitialized)
                    {
                        var nowTicks = globalTime.GetNowTicks();
                        eventLoggerAdditionalInfoRepository.GetOrCreateFirstEventInfo(new EventInfo
                            {
                                Shard = "Null",
                                Ticks = nowTicks,
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

        private EventInfo[] WriteBatch(EventStorageElement[] eventBatch)
        {
            var random = new Random(Guid.NewGuid().GetHashCode());
            var dict = eventBatch.ToDictionary(x => x.EventInfo.Id);
            var result = new List<EventInfo>();

            var batchForWrite = eventBatch;
            for(var attempt = 0; !wasDisposed && attempt < 10; ++attempt)
            {
                using(var deferredResult = queueRaker.Enqueue(batchForWrite, attempt))
                {
                    deferredResult.WaitFinished();
                    batchForWrite = deferredResult.failureIds.Select(x => dict[x]).ToArray();
                    result.AddRange(deferredResult.successInfos);
                    if(batchForWrite.Length == 0) return result.ToArray();
                }
                var sleepTime = random.Next(5 * (int)Math.Exp(Math.Min(attempt, 10)));
                Thread.Sleep(sleepTime);
                if(attempt > 1)
                    logger.Warn(string.Format("Big attempt: attempt = {0}, sleepTime = {1}", attempt, sleepTime));
            }

            if(wasDisposed)
                throw new CouldNotWriteBoxEventException("This instance of eventLogger was disposed");
            throw new CouldNotWriteBoxEventException("Could not write in 10 attempts");
        }

        private IEnumerable<EventLogRecord> InnerReadEvents(EventInfo startEventInfo, EventInfo finishEventInfo, string[] shards, int batchCount)
        {
            var startRowNumber = eventLogPointerCreator.GetRowNumber(startEventInfo);
            var finishRowNumber = eventLogPointerCreator.GetRowNumber(finishEventInfo);
            var startEventPointer = eventLogPointerCreator.Create(startEventInfo);
            var finishEventPointer = eventLogPointerCreator.Create(finishEventInfo);

            var getRowsBatchCount = batchCount / shards.Length;
            var rowKeys = shards.Select(shard => eventLogPointerCreator.ChangeShard(startRowNumber, shard)).ToArray();
            var rows1 = columnFamilyConnection.GetRowsExclusive(rowKeys, startEventPointer.ColumnName, getRowsBatchCount).ToDictionary(x => x.Key, x => x.Value);
            var rows2 = new Dictionary<string, Column[]>();
            var needColumns2 = false;
            if(startRowNumber < finishRowNumber && rows1.Count(x => x.Value.Length < getRowsBatchCount) > 2) //todo подумать над константой
            {
                var rowKeys2 = shards.Select(shard => eventLogPointerCreator.ChangeShard(startRowNumber + 1, shard)).ToArray();
                rows2 = columnFamilyConnection.GetRowsExclusive(rowKeys2, null, getRowsBatchCount).ToDictionary(x => x.Key, x => x.Value);
                needColumns2 = true;
            }

            var list = new List<IEnumerable<EventLogRecord>>();
            foreach(var shard in shards)
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
            Column[] columns;
            if(!rows.TryGetValue(rowKey, out columns) || columns == null)
                columns = new Column[0];
            return columns;
        }

        private IEnumerable<EventLogRecord> GetEventsEnumerable(string shard, long startRowNumber, string startEventColumnName, long finishRowNumber, Column[][] columns, int batchCount, int getRowsBatchCount)
        {
            for(var i = startRowNumber; i <= finishRowNumber; i++)
            {
                string currentExclusiveColumn = null;

                if(i - startRowNumber < columns.Length)
                {
                    var cols = columns[i - startRowNumber];
                    currentExclusiveColumn = cols.Length > 0 ? cols.Last().Name : (i == startRowNumber ? startEventColumnName : null);

                    foreach(var column in cols)
                        yield return GetEventLogRecord(column);
                    if(cols.Length < getRowsBatchCount)
                        continue;
                }

                var currentRowKey = eventLogPointerCreator.ChangeShard(i, shard);
                while(true)
                {
                    var cols = columnFamilyConnection.GetColumns(currentRowKey, currentExclusiveColumn, batchCount);
                    if(cols.Length == 0) break;
                    foreach(var column in cols)
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

        private readonly ILog logger = LogManager.GetLogger(typeof(EventLogger));

        private readonly ISerializer serializer;
        private readonly IEventInfoRepository eventInfoRepository;
        private readonly IEventLogPointerCreator eventLogPointerCreator;
        private readonly Func<IQueueRaker> createQueueRaker;
        private readonly IEventLoggerAdditionalInfoRepository eventLoggerAdditionalInfoRepository;
        private readonly IGlobalTime globalTime;
        private readonly IColumnFamilyConnection columnFamilyConnection;

        private IQueueRaker queueRaker;

        private volatile bool wasDisposed;
        private volatile bool wasInitialized;
        private readonly object lockObject = new object();
    }
}