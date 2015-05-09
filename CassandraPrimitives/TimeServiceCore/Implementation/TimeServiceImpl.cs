using System;

using GroBuf;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Connections;
using SKBKontur.Catalogue.CassandraPrimitives.TimeServiceCore.Settings;

namespace SKBKontur.Catalogue.CassandraPrimitives.TimeServiceCore.Implementation
{
    public class TimeServiceImpl : ITimeServiceImpl
    {
        public TimeServiceImpl(ICassandraCluster cassandraCluster, ITimeServiceSettings timeServiceSettings, ISerializer serializer)
        {
            this.serializer = serializer;
            connection = cassandraCluster.RetrieveColumnFamilyConnection(timeServiceSettings.Keyspace, timeServiceSettings.ColumnFamily);
        }

        public void UpdateTime()
        {
            var localNow = DateTime.UtcNow.Ticks;
            var globalNow = GetNowTicks();
            var result = Math.Max(localNow, globalNow);
            UpdateTime(result);
        }

        private void UpdateTime(long ticks)
        {
            connection.AddColumn(rowName, new Column
            {
                Name = columnName,
                Timestamp = ticks,
                Value = serializer.Serialize(ticks),
            });
        }

        public long GetNowTicks()
        {
            Column column;
            if(connection.TryGetColumn(rowName, columnName, out column))
                return serializer.Deserialize<long>(column.Value);
            var now = DateTime.UtcNow.Ticks;
            UpdateTime(now);
            return now;
        }

        private readonly ISerializer serializer;
        private readonly IColumnFamilyConnection connection;
        private const string rowName = "TicksRow";
        private const string columnName = "TicksColumn";
    }
}