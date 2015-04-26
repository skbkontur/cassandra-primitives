using System;
using System.Linq;
using System.Threading;

using GroBuf;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Connections;

namespace SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.MetaStorage
{
    public class MetaStorage : IMetaStorage
    {
        public MetaStorage(ICassandraCluster cassandraCluster, ISerializer serializer, RemoteLockSettings lockSettings)
        {
            this.cassandraCluster = cassandraCluster;
            this.serializer = serializer;
            this.lockSettings = lockSettings;
        }

        public LockMeta[] GetMetas(string lockId)
        {
            var rowName = GetMetaRowName(lockId);
            var result = new LockMeta[0];
            MakeInConnection(connection => result = connection.GetRow(rowName).Select(x => serializer.Deserialize<LockMeta>(x.Value)).ToArray());
            return result;
        }

        public string GetActualRowKey(string lockId)
        {
            var metas = GetMetas(lockId);
            var actualMeta = metas.LastOrDefault();
            if(actualMeta == null)
            {
                actualMeta = new LockMeta
                {
                    Count = 0,
                    Index = 0,
                    LockId = lockId,
                    ColumnName = GetMetaColumnName(lockId, 0),
                };
            }
            if(actualMeta.Count >= lockSettings.MaxRowLength)
            {
                actualMeta = new LockMeta
                {
                    Count = 0,
                    Index = actualMeta.Index + 1,
                    LockId = lockId,
                    ColumnName = GetMetaColumnName(lockId, actualMeta.Index + 1),
                };
            }
            actualMeta.Count++;
            WriteMeta(actualMeta);
            return GetMetaColumnName(lockId, actualMeta.Index);
        }

        public void DeleteMeta(string lockId, LockMeta meta)
        {
            MakeInConnection(connection => connection.DeleteColumn(GetMetaRowName(lockId), meta.ColumnName));
        }

        public void WriteMeta(LockMeta meta)
        {
            var rowName = GetMetaRowName(meta.LockId);
            MakeInConnection(connection => connection.AddColumn(rowName, new Column
            {
                Name = GetMetaColumnName(meta.LockId, meta.Index),
                Timestamp = GetNowTicks(),
                Value = serializer.Serialize(meta),
            }));
        }

        private void MakeInConnection(Action<IColumnFamilyConnection> action)
        {
            var connection = cassandraCluster.RetrieveColumnFamilyConnection(lockSettings.KeyspaceName, lockSettings.ColumnFamilyName);
            action(connection);
        }

        private string GetMetaRowName(string lockId)
        {
            return string.Format("LockMeta_{0}", lockId);
        }

        private long GetNowTicks()
        {
            var ticks = DateTime.UtcNow.Ticks;
            while(true)
            {
                var last = Interlocked.Read(ref lastTicks);
                var cur = Math.Max(ticks, last + 1);
                if(Interlocked.CompareExchange(ref lastTicks, cur, last) == last)
                    return cur;
            }
        }

        private string GetMetaColumnName(string lockId, int index)
        {
            return string.Format("{0}_{1:D10}", lockId, index);
        }

        private readonly ICassandraCluster cassandraCluster;
        private readonly ISerializer serializer;
        private readonly RemoteLockSettings lockSettings;

        private long lastTicks;
    }
}