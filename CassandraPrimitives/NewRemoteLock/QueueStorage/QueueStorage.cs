using System;
using System.Linq;
using System.Threading;

using GroBuf;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Connections;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.MetaStorage;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.Primitives;

namespace SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.QueueStorage
{
    public class QueueStorage : IQueueStorage
    {
        public QueueStorage(IMetaStorage metaStorage, ICassandraCluster cassandraCluster, ISerializer serializer, RemoteLockSettings lockSettings)
        {
            this.metaStorage = metaStorage;
            this.cassandraCluster = cassandraCluster;
            this.serializer = serializer;
            this.lockSettings = lockSettings;
        }

        public string Add(string lockId, string threadId, long timestamp)
        {
            var rowName = metaStorage.GetActualRowKey(lockId);
            WriteQueueElement(rowName, threadId, timestamp);
            return rowName;
        }

        public void ExtendRent(string lockId, string rowName, string threadId, long timestamp)
        {
            WriteQueueElement(rowName, threadId, timestamp);
        }

        public void Remove(string lockId, string rowName, string threadId, long timestamp)
        {
            var columnName = GetQueueColumnName(threadId, timestamp);
            MakeInConnection(connection => connection.DeleteColumn(rowName, columnName));
        }

        public string GetFirstElement(string lockId)
        {
            var metas = metaStorage.GetMetas(lockId);
            foreach(var meta in metas)
            {
                var columnName = meta.ColumnName;
                var threads = GetThreadsInRow(columnName);
                if(threads.Any())
                    return threads.First();
                metaStorage.DeleteMeta(lockId, meta);
            }
            return null;
        }

        private string[] GetThreadsInRow(string row)
        {
            var result = new string[0];
            MakeInConnection(connection => { result = connection.GetRow(row).Select(x => serializer.Deserialize<string>(x.Value)).ToArray(); });
            return result;
        }

        private string GetQueueColumnName(string threadId, long timestamp)
        {
            return string.Format("{0:D20}_{1}", timestamp, threadId);
        }

        private void MakeInConnection(Action<IColumnFamilyConnection> action)
        {
            var connection = cassandraCluster.RetrieveColumnFamilyConnection(lockSettings.KeyspaceName, lockSettings.ColumnFamilyName);
            action(connection);
        }

        private void WriteQueueElement(string rowName, string threadId, long timestamp)
        {
            var columnName = GetQueueColumnName(threadId, timestamp);
            MakeInConnection(connection => connection.AddColumn(rowName, new Column
            {
                Name = columnName,
                Timestamp = GetNowTicks(),
                Value = serializer.Serialize(threadId),
                TTL = TimeSpan.FromMilliseconds(lockSettings.LockTTL).Seconds,
            }));
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
        private readonly ISerializer serializer;
        private readonly RemoteLockSettings lockSettings;
    }
}