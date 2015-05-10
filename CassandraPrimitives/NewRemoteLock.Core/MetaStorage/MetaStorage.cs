using System;
using System.Linq;

using GroBuf;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Connections;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.Core.Settings;

namespace SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.Core.MetaStorage
{
    public class MetaStorage : IMetaStorage
    {
        public MetaStorage(ITimeGetter timeGetter, ICassandraCluster cassandraCluster, ISerializer serializer, RemoteLockSettings lockSettings)
        {
            this.timeGetter = timeGetter;
            this.cassandraCluster = cassandraCluster;
            this.serializer = serializer;
            this.lockSettings = lockSettings;
        }

        public LockMeta[] GetMetas(string lockId)
        {
            var rowName = NamesProvider.GetMetaRowName(lockId);
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
                    ColumnName = NamesProvider.GetMetaColumnName(lockId, 0),
                };
            }
            if(actualMeta.Count >= lockSettings.MaxRowLength)
            {
                actualMeta = new LockMeta
                {
                    Count = 0,
                    Index = actualMeta.Index + 1,
                    LockId = lockId,
                    ColumnName = NamesProvider.GetMetaColumnName(lockId, actualMeta.Index + 1),
                };
            }
            actualMeta.Count++;
            WriteMeta(actualMeta);
            return NamesProvider.GetMetaColumnName(lockId, actualMeta.Index);
        }

        public void DeleteMeta(string lockId, LockMeta meta)
        {
            MakeInConnection(connection => connection.DeleteColumn(NamesProvider.GetMetaRowName(lockId), meta.ColumnName));
        }

        public void WriteMeta(LockMeta meta)
        {
            var rowName = NamesProvider.GetMetaRowName(meta.LockId);
            MakeInConnection(connection => connection.AddColumn(rowName, new Column
            {
                Name = NamesProvider.GetMetaColumnName(meta.LockId, meta.Index),
                Timestamp = timeGetter.GetNowTicks(),
                Value = serializer.Serialize(meta),
            }));
        }

        private void MakeInConnection(Action<IColumnFamilyConnection> action)
        {
            var connection = cassandraCluster.RetrieveColumnFamilyConnection(lockSettings.KeyspaceName, lockSettings.ColumnFamilyName);
            action(connection);
        }

        private readonly ITimeGetter timeGetter;
        private readonly ICassandraCluster cassandraCluster;
        private readonly ISerializer serializer;
        private readonly RemoteLockSettings lockSettings;
    }
}