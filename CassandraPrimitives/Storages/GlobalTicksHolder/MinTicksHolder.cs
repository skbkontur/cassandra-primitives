using System;
using System.Collections.Concurrent;

using GroBuf;

using JetBrains.Annotations;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Connections;

namespace SKBKontur.Catalogue.CassandraPrimitives.Storages.GlobalTicksHolder
{
    public class MinTicksHolder
    {
        public MinTicksHolder(ISerializer serializer, IColumnFamilyConnection minTicksConnection)
        {
            this.serializer = serializer;
            this.minTicksConnection = minTicksConnection;
        }

        public long? GetMinTicks([NotNull] string key)
        {
            Column column;
            if(!minTicksConnection.TryGetColumn(key, ticksColumnName, out column))
                return null;
            return long.MaxValue - serializer.Deserialize<long>(column.Value);
        }

        public void UpdateMinTicks([NotNull] string key, long ticks)
        {
            long minTicks;
            if(persistedMinTicks.TryGetValue(key, out minTicks) && ticks >= minTicks)
                return;
            minTicksConnection.AddColumn(key, new Column
                {
                    Name = ticksColumnName,
                    Timestamp = long.MaxValue - ticks,
                    Value = serializer.Serialize(long.MaxValue - ticks),
                    TTL = null,
                });
            persistedMinTicks.AddOrUpdate(key, ticks, (k, oldMinTicks) => Math.Min(ticks, oldMinTicks));
        }

        public void ResetInMemoryState()
        {
            persistedMinTicks.Clear();
        }

        private const string ticksColumnName = "Ticks";
        private readonly ISerializer serializer;
        private readonly IColumnFamilyConnection minTicksConnection;
        private readonly ConcurrentDictionary<string, long> persistedMinTicks = new ConcurrentDictionary<string, long>();
    }
}