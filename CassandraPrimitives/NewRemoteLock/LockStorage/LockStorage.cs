using System;
using System.Linq;
using System.Threading;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Connections;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.MetaStorage;

namespace SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.LockStorage
{
    public class LockStorage : ILockStorage
    {
        public LockStorage(IMetaStorage metaStorage, ICassandraCluster cassandraCluster, RemoteLockSettings lockSettings)
        {
            this.metaStorage = metaStorage;
            this.cassandraCluster = cassandraCluster;
            this.lockSettings = lockSettings;
        }

        public void AddThreadToLock(string lockId, string rowName, string threadId)
        {
            MakeInConnection(connection => connection.AddColumn(GetLockRowName(rowName), new Column
            {
                Name = threadId,
                Value = new byte[0],
                Timestamp = GetNowTicks(),
                TTL = TimeSpan.FromMilliseconds(lockSettings.LockTTL).Seconds,
            }));
        }

        public void ExtendRent(string lockId, string rowName, string threadId)
        {
            AddThreadToLock(lockId, rowName, threadId);
        }

        public void RemoveThreadFromLock(string lockId, string rowName, string threadId)
        {
            MakeInConnection(connection => connection.DeleteColumn(GetLockRowName(rowName), threadId));
        }

        public int GetLocksCount(string lockId)
        {
            var metas = metaStorage.GetMetas(lockId);
            var result = 0;
            MakeInConnection(connection => result = connection.GetRowsExclusive(metas.Select(x => GetLockRowName(x.ColumnName)), "", 2).Select(x => x.Value.Count()).Sum());
            return result;
        }

        private void MakeInConnection(Action<IColumnFamilyConnection> action)
        {
            var connection = cassandraCluster.RetrieveColumnFamilyConnection(lockSettings.KeyspaceName, lockSettings.ColumnFamilyName);
            action(connection);
        }

        private string GetLockRowName(string rowName)
        {
            return string.Format("Lock_{0}", rowName);
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

        private long lastTicks;
        private readonly IMetaStorage metaStorage;
        private readonly ICassandraCluster cassandraCluster;
        private readonly RemoteLockSettings lockSettings;
    }
}