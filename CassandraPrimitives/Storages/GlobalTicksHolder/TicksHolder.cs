using GroBuf;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Connections;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.Primitives;

namespace SKBKontur.Catalogue.CassandraPrimitives.Storages.GlobalTicksHolder
{
    public class TicksHolder : ITicksHolder
    {
        public TicksHolder(ISerializer serializer, ICassandraCluster cassandraCluster, ColumnFamilyFullName columnFamilyPath)
        {
            this.serializer = serializer;
            connection = cassandraCluster.RetrieveColumnFamilyConnection(columnFamilyPath.KeyspaceName, columnFamilyPath.ColumnFamilyName);
        }

        public long UpdateMaxTicks(string name, long ticks)
        {
            connection.AddColumn(name, new Column
                {
                    Name = maxTicksColumnName,
                    Timestamp = ticks,
                    Value = serializer.Serialize(ticks)
                });
            return GetMaxTicks(name);
        }

        public long GetMaxTicks(string name)
        {
            if (!connection.TryGetColumn(name, maxTicksColumnName, out var column))
                return 0;
            return serializer.Deserialize<long>(column.Value);
        }

        public long UpdateMinTicks(string name, long ticks)
        {
            connection.AddColumn(name, new Column
                {
                    Name = minTicksColumnName,
                    Timestamp = long.MaxValue - ticks,
                    Value = serializer.Serialize(long.MaxValue - ticks)
                });
            return GetMinTicks(name);
        }

        public long GetMinTicks(string name)
        {
            if (!connection.TryGetColumn(name, minTicksColumnName, out var column))
                return 0;
            return long.MaxValue - serializer.Deserialize<long>(column.Value);
        }

        private const string maxTicksColumnName = "MaxTicks";
        private const string minTicksColumnName = "MinTicks";

        private readonly ISerializer serializer;
        private readonly IColumnFamilyConnection connection;
    }
}