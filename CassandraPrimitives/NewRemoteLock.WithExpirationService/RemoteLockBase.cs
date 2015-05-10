using System;
using System.Collections.Concurrent;
using System.Threading;

using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.Core;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.Core.LockCreatorStorage;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.Core.Settings;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLockBase;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.ExpirationMonitoringStorage;

namespace SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithExpirationService
{
    public abstract class RemoteLockBase : IRemoteLock
    {
        protected RemoteLockBase(string lockId, ILockCreatorStorage lockCreatorStorage, IExpirationMonitoringStorage expirationMonitoringStorage, ITimeGetter timeGetter, RemoteLockSettings remoteLockSettings)
        {
            this.lockCreatorStorage = lockCreatorStorage;
            this.expirationMonitoringStorage = expirationMonitoringStorage;
            this.remoteLockSettings = remoteLockSettings;
            LockId = lockId;
            ThreadId = Guid.NewGuid().ToString();
            Timestamp = timeGetter.GetNowTicks();
        }

        public void Dispose()
        {
            RemoveThreadFromLock();
            RemoveThreadFromQueue();
            if(remoteLockSettings.UseLocalOptimization)
                RemoveFromLocal();
        }

        public string LockId { get; private set; }
        public string ThreadId { get; private set; }

        protected bool IsFirstInQueue()
        {
            return lockCreatorStorage.GetFirstInQueue(LockId) == ThreadId;
        }

        protected bool IsLockFree()
        {
            if(remoteLockSettings.UseLocalOptimization)
            {
                if(localLocks.GetOrAdd(LockId, ThreadId) != ThreadId)
                    return false;
            }
            return lockCreatorStorage.GetThreadsCountInLock(LockId) == 0;
        }

        protected bool AmITheOnlyOwnerOfLock()
        {
            return lockCreatorStorage.GetThreadsCountInLock(LockId) == 1;
        }

        protected void AddThreadToQueue()
        {
            RowName = lockCreatorStorage.AddThreadToQueue(LockId, ThreadId, Timestamp);
            expirationMonitoringStorage.AddEntry(new ExpiringObjectMeta
            {
                Keyspace = remoteLockSettings.KeyspaceName,
                ColumnFamily = remoteLockSettings.ColumnFamilyName,
                Row = NamesProvider.GetQueueRowName(RowName),
                Column = NamesProvider.GetQueueColumnName(ThreadId, Timestamp),
            }, Timestamp);
            queueRentEvent = new ManualResetEvent(false);
            queueRentEvent.Reset();
            queueRenter = new Thread(UpdateQueueRent);
            queueRenter.Start();
        }

        protected void RemoveThreadFromQueue()
        {
            lockCreatorStorage.RemoveThreadFromQueue(LockId, RowName, ThreadId, Timestamp);
            expirationMonitoringStorage.DeleteEntry(new ExpiringObjectMeta
            {
                Keyspace = remoteLockSettings.KeyspaceName,
                ColumnFamily = remoteLockSettings.ColumnFamilyName,
                Row = NamesProvider.GetQueueRowName(RowName),
                Column = NamesProvider.GetQueueColumnName(ThreadId, Timestamp),
            }, Timestamp);
            queueRentEvent.Set();
            queueRenter.Join();
        }

        protected void AddThreadToLock()
        {
            lockCreatorStorage.AddThreadToLock(LockId, RowName, ThreadId);
            expirationMonitoringStorage.AddEntry(new ExpiringObjectMeta
            {
                Keyspace = remoteLockSettings.KeyspaceName,
                ColumnFamily = remoteLockSettings.ColumnFamilyName,
                Row = NamesProvider.GetLockRowName(RowName),
                Column = NamesProvider.GetLockColumnName(ThreadId),
            }, Timestamp);
            lockRentEvent = new ManualResetEvent(false);
            lockRentEvent.Reset();
            lockRenter = new Thread(UpdateLockRent);
            lockRenter.Start();
        }

        protected void RemoveThreadFromLock()
        {
            lockCreatorStorage.RemoveThreadFromLock(LockId, RowName, ThreadId);
            expirationMonitoringStorage.DeleteEntry(new ExpiringObjectMeta
            {
                Keyspace = remoteLockSettings.KeyspaceName,
                ColumnFamily = remoteLockSettings.ColumnFamilyName,
                Row = NamesProvider.GetLockRowName(RowName),
                Column = NamesProvider.GetLockColumnName(ThreadId),
            }, Timestamp);
            lockRentEvent.Set();
            lockRenter.Join();
        }

        protected void RemoveFromLocal()
        {
            while(true)
            {
                string threadId;
                if(!localLocks.TryGetValue(LockId, out threadId) || threadId != ThreadId)
                    break;
                if(localLocks.TryRemove(LockId, out threadId))
                    break;
            }
        }

        private void UpdateQueueRent()
        {
            while(true)
            {
                if(queueRentEvent.WaitOne(remoteLockSettings.ExtendRentPeriod))
                    break;
                lockCreatorStorage.ExtendQueueRent(LockId, RowName, ThreadId, Timestamp);
            }
        }

        private void UpdateLockRent()
        {
            while(true)
            {
                if(lockRentEvent.WaitOne(remoteLockSettings.ExtendRentPeriod))
                    break;
                lockCreatorStorage.ExtendLockRent(LockId, RowName, ThreadId);
            }
        }

        private string RowName { get; set; }
        private long Timestamp { get; set; }
        private static readonly ConcurrentDictionary<string, string> localLocks = new ConcurrentDictionary<string, string>();

        private readonly RemoteLockSettings remoteLockSettings;
        private readonly ILockCreatorStorage lockCreatorStorage;
        private readonly IExpirationMonitoringStorage expirationMonitoringStorage;

        private Thread queueRenter;
        private ManualResetEvent queueRentEvent;
        private Thread lockRenter;
        private ManualResetEvent lockRentEvent;
    }
}