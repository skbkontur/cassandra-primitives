using GroBuf;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Connections;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Exceptions;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Primitives;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.Primitives;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.Implementation
{
    internal class EventLoggerAdditionalInfoRepository : IEventLoggerAdditionalInfoRepository
    {
        public EventLoggerAdditionalInfoRepository(
            ICassandraCluster cassandraCluster,
            ISerializer serializer,
            IRemoteLockCreator remoteLockCreator,
            ColumnFamilyFullName additionalInfoColumnFamily,
            ColumnFamilyFullName eventLogColumnFamily)
        {
            this.eventLogColumnFamily = eventLogColumnFamily;
            this.serializer = serializer;
            this.remoteLockCreator = remoteLockCreator;
            columnFamilyConnection = cassandraCluster.RetrieveColumnFamilyConnection(additionalInfoColumnFamily.KeyspaceName, additionalInfoColumnFamily.ColumnFamilyName);
        }

        public EventInfo GetFirstEventInfo()
        {
            if (localFirstEventInfo == null)
                throw new EventLoggerNeedInitializationException();
            return localFirstEventInfo;
        }

        public EventInfo GetOrCreateFirstEventInfo(EventInfo eventInfo)
        {
            if (localFirstEventInfo == null)
            {
                var lockId = $"eventLoggerFirstEventWriting_{eventLogColumnFamily.KeyspaceName}_{eventLogColumnFamily.ColumnFamilyName}";
                using (remoteLockCreator.Lock(lockId))
                {
                    var result = ReadEventInfo(firstEventInfoRow, firstEventInfoColumn);
                    if (result == null)
                    {
                        result = eventInfo;
                        WriteEventInfo(eventInfo, firstEventInfoRow, firstEventInfoColumn);
                        WriteEventInfo(eventInfo, lastGoodEventInfoRow, lastGoodEventInfoColumn);
                    }
                    localFirstEventInfo = result;
                }
            }
            return localFirstEventInfo;
        }

        public void SetLastEventInfo(EventInfo eventInfo)
        {
            var needWriting = false;
            lock (lockObject)
            {
                if (localLastGoodEventInfo.CompareTo(eventInfo) < 0)
                {
                    localLastGoodEventInfo = eventInfo;
                    needWriting = true;
                }
            }
            if (needWriting)
                WriteEventInfo(eventInfo, lastGoodEventInfoRow, lastGoodEventInfoColumn);
        }

        public EventInfo GetGoodLastEventInfo()
        {
            var globalLastEventInfo = ReadEventInfo(lastGoodEventInfoRow, lastGoodEventInfoColumn);
            if (globalLastEventInfo == null)
                return localLastGoodEventInfo;
            lock (lockObject)
            {
                if (localLastGoodEventInfo == null || localLastGoodEventInfo.CompareTo(globalLastEventInfo) < 0)
                    localLastGoodEventInfo = globalLastEventInfo;
            }
            return localLastGoodEventInfo;
        }

        private EventInfo ReadEventInfo(string rowKey, string columnName)
        {
            Column column;
            if (!columnFamilyConnection.TryGetColumn(rowKey, columnName, out column))
                return null;
            return serializer.Deserialize<EventInfo>(column.Value);
        }

        private void WriteEventInfo(EventInfo eventInfo, string rowKey, string columnName, bool winMax = true)
        {
            columnFamilyConnection.AddColumn(
                rowKey,
                new Column
                    {
                        Name = columnName,
                        Value = serializer.Serialize(eventInfo),
                        Timestamp = winMax ? eventInfo.Ticks : long.MaxValue - eventInfo.Ticks
                    });
        }

        private const string firstEventInfoRow = "firstEvent";
        private const string firstEventInfoColumn = "firstEventColumn";

        private const string lastGoodEventInfoRow = "lastGoodEvent";
        private const string lastGoodEventInfoColumn = "lastGoodEventColumn";

        private readonly ColumnFamilyFullName eventLogColumnFamily;
        private readonly ISerializer serializer;
        private readonly IRemoteLockCreator remoteLockCreator;
        private readonly IColumnFamilyConnection columnFamilyConnection;

        private volatile EventInfo localFirstEventInfo;
        private volatile EventInfo localLastGoodEventInfo;
        private readonly object lockObject = new object();
    }
}