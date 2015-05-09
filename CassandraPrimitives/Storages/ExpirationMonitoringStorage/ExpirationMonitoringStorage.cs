using System;
using System.Collections.Generic;
using System.Linq;

using GroBuf;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Connections;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.Primitives;

namespace SKBKontur.Catalogue.CassandraPrimitives.Storages.ExpirationMonitoringStorage
{
    public class ExpirationMonitoringStorage : IExpirationMonitoringStorage
    {
        public ExpirationMonitoringStorage(ICassandraCluster cassandraCluster, ISerializer serializer, ColumnFamilyFullName columnFamilyFullName)
        {
            this.serializer = serializer;
            connection = cassandraCluster.RetrieveColumnFamilyConnection(columnFamilyFullName.KeyspaceName, columnFamilyFullName.ColumnFamilyName);
            ticksInRow = TimeSpan.FromMinutes(6).Ticks;
        }

        public void AddEntry(ExpiringObjectMeta meta, long ticks)
        {
            connection.AddColumn(GetRowName(RoundTicks(ticks)), new Column
            {
                Name = GetColumnName(meta),
                Timestamp = ticks,
                Value = serializer.Serialize(meta),
            });
        }

        public void DeleteEntry(ExpiringObjectMeta meta, long ticks)
        {
            connection.DeleteColumn(GetRowName(RoundTicks(ticks)), GetColumnName(meta), ticks + 1);
        }

        public ExpiringObjectMeta[] GetEntries(long fromTicks, long toTicks)
        {
            var rows = new List<string>();
            var fromRounded = RoundTicks(fromTicks);
            var toRounded = RoundTicks(toTicks);
            for(var i = fromRounded; i <= toRounded; i++)
                rows.Add(GetRowName(i));
            var columns = connection.GetRowsExclusive(rows, "", int.MaxValue - 2)
                .SelectMany(x => x.Value);
            return columns
                .Where(x => fromTicks < x.Timestamp && x.Timestamp <= toTicks)
                .Select(x => serializer.Deserialize<ExpiringObjectMeta>(x.Value))
                .ToArray();
        }

        private string GetColumnName(ExpiringObjectMeta meta)
        {
            return string.Format("{0}_{1}_{2}_{3}", meta.Keyspace, meta.ColumnFamily, meta.Row, meta.Column);
        }

        private long RoundTicks(long ticks)
        {
            return ticks / ticksInRow;
        }

        private string GetRowName(long ticks)
        {
            return string.Format("{0:D20}", ticks);
        }

        private readonly ISerializer serializer;

        private readonly long ticksInRow;
        private readonly IColumnFamilyConnection connection;
    }
}