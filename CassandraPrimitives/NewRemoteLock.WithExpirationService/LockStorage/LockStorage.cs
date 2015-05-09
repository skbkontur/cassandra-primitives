using System;
using System.Linq;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Connections;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithExpirationService.LockCreatorStorage;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithExpirationService.MetaStorage;
using SKBKontur.Catalogue.CassandraPrimitives.TimeServiceClient;

namespace SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithExpirationService.LockStorage
{
    public class LockStorage : ILockStorage
    {
        public LockStorage(ITimeServiceClient timeServiceClient, IMetaStorage metaStorage, ICassandraCluster cassandraCluster, RemoteLockSettings lockSettings)
        {
            this.timeServiceClient = timeServiceClient;
            this.metaStorage = metaStorage;
            this.cassandraCluster = cassandraCluster;
            this.lockSettings = lockSettings;
        }

        public void AddThreadToLock(string lockId, string rowName, string threadId)
        {
            MakeInConnection(connection => connection.AddColumn(NamesProvider.GetLockRowName(rowName), new Column
            {
                Name = NamesProvider.GetLockColumnName(threadId),
                Value = new byte[0],
                Timestamp = GetNowTicks(),
            }));
        }

        public void ExtendRent(string lockId, string rowName, string threadId)
        {
            AddThreadToLock(lockId, rowName, threadId);
        }

        public void RemoveThreadFromLock(string lockId, string rowName, string threadId)
        {
            MakeInConnection(connection => connection.DeleteColumn(NamesProvider.GetLockRowName(rowName), NamesProvider.GetLockColumnName(threadId)));
        }

        public int GetLocksCount(string lockId)
        {
            var metas = metaStorage.GetMetas(lockId);
            var result = 0;
            MakeInConnection(connection => result = connection.GetRowsExclusive(metas.Select(x => NamesProvider.GetLockRowName(x.ColumnName)), "", 2).Select(x => x.Value.Count()).Sum());
            return result;
        }

        private void MakeInConnection(Action<IColumnFamilyConnection> action)
        {
            var connection = cassandraCluster.RetrieveColumnFamilyConnection(lockSettings.KeyspaceName, lockSettings.ColumnFamilyName);
            action(connection);
        }

        private long GetNowTicks()
        {
            return timeServiceClient.GetNowTicks();
        }

        private readonly ITimeServiceClient timeServiceClient;
        private readonly IMetaStorage metaStorage;
        private readonly ICassandraCluster cassandraCluster;
        private readonly RemoteLockSettings lockSettings;
    }
}