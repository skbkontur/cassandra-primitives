using System;
using System.Globalization;

using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Primitives;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.Implementation
{
    internal class EventLogPointerCreator : IEventLogPointerCreator
    {
        public EventPointer Create(EventInfo eventInfo, string specificShard = null)
        {
            var shard = specificShard ?? eventInfo.Shard;
            var rowNumber = eventInfo.Ticks / ticksPartition;
            return new EventPointer
                {
                    RowKey = GetRowKey(rowNumber, shard),
                    ColumnName = GetColumnName(eventInfo.Id, eventInfo.Ticks)
                };
        }

        public long GetRowNumber(EventInfo eventInfo)
        {
            return eventInfo.Ticks / ticksPartition;
        }

        public EventPointer ToNextRow(string rowKey)
        {
            var rowKeyInfo = ParseRowKey(rowKey);
            var newRowNumber = rowKeyInfo.RowNumber + 1;
            return new EventPointer
                {
                    RowKey = GetRowKey(newRowNumber, rowKeyInfo.Shard),
                    ColumnName = GetColumnName(null, newRowNumber * ticksPartition),
                };
        }

        public string ChangeShard(string rowKey, string shard)
        {
            var rowKeyInformation = ParseRowKey(rowKey);
            return GetRowKey(rowKeyInformation.RowNumber, shard);
        }

        public string ChangeShard(long rowNumber, string shard)
        {
            return GetRowKey(rowNumber, shard);
        }

        public string GetShard(string rowKey)
        {
            return ParseRowKey(rowKey).Shard;
        }

        public long GetEventTicks(EventPointer eventPointer)
        {
            return ParseColumnName(eventPointer.ColumnName).Ticks;
        }

        private static string GetRowKey(long rowNumber, string shard)
        {
            return rowNumber.ToString("D20", CultureInfo.InvariantCulture) + "_" + shard;
        }

        private static string GetColumnName(EventId eventId, long ticks)
        {
            return ticks.ToString("D20", CultureInfo.InvariantCulture) + "_" + GetIdString(eventId);
        }

        private static string GetIdString(EventId eventId)
        {
            if(eventId == null) return "";
            return eventId.ScopeId + "_" + eventId.Id;
        }

        private ColumnNameInformation ParseColumnName(string columnName)
        {
            var args = columnName.Split('_');
            if(args.Length != 3) throw new Exception("BUG");
            return new ColumnNameInformation(long.Parse(args[0]), args[1], args[2]);
        }

        private RowKeyInformation ParseRowKey(string rowKey)
        {
            var idx = rowKey.IndexOf('_');
            if (idx == -1) throw new Exception("BUG");
            var ticksPartitionToken = rowKey.Substring(0, idx);
            var shardToken = rowKey.Substring(idx+1);
            return new RowKeyInformation(long.Parse(ticksPartitionToken), shardToken);
        }

        private class RowKeyInformation
        {
            public RowKeyInformation(long rowNumber, string shard)
            {
                Shard = shard;
                RowNumber = rowNumber;
            }

            public string Shard { get; private set; }
            public long RowNumber { get; private set; }
        }

        private class ColumnNameInformation
        {
            public ColumnNameInformation(long ticks, string eventScopeId, string eventId)
            {
                Ticks = ticks;
                EventScopeId = eventScopeId;
                EventId = eventId;
            }

            public long Ticks { get; private set; }
            public string EventScopeId { get; private set; }
            public string EventId { get; private set; }
        }

        private const long ticksPartition = TimeSpan.TicksPerMinute * 10;
    }
}