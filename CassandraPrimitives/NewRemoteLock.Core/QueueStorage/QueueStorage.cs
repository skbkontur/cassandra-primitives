using System;
using System.Linq;

using GroBuf;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Connections;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.Core.MetaStorage;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.Core.Settings;

namespace SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.Core.QueueStorage
{
    public class QueueStorage : IQueueStorage
    {
        public QueueStorage(ITimeGetter timeGetter, IMetaStorage metaStorage, ICassandraCluster cassandraCluster, ISerializer serializer, RemoteLockSettings lockSettings)
        {
            this.timeGetter = timeGetter;
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
            var columnName = NamesProvider.GetQueueColumnName(threadId, timestamp);
            MakeInConnection(connection => connection.DeleteColumn(NamesProvider.GetQueueRowName(rowName), columnName));
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

        private void MakeInConnection(Action<IColumnFamilyConnection> action)
        {
            var connection = cassandraCluster.RetrieveColumnFamilyConnection(lockSettings.KeyspaceName, lockSettings.ColumnFamilyName);
            action(connection);
        }

        private void WriteQueueElement(string rowName, string threadId, long timestamp)
        {
            var columnName = NamesProvider.GetQueueColumnName(threadId, timestamp);
            MakeInConnection(connection => connection.AddColumn(NamesProvider.GetQueueRowName(rowName), new Column
            {
                Name = columnName,
                Timestamp = timeGetter.GetNowTicks(),
                Value = serializer.Serialize(threadId),
                TTL = lockSettings.LockTTL.HasValue ? (int?)TimeSpan.FromMilliseconds(lockSettings.LockTTL.Value).Seconds : null,
            }));
        }

        private readonly ITimeGetter timeGetter;
        private readonly IMetaStorage metaStorage;
        private readonly ICassandraCluster cassandraCluster;
        private readonly ISerializer serializer;
        private readonly RemoteLockSettings lockSettings;
    }
}