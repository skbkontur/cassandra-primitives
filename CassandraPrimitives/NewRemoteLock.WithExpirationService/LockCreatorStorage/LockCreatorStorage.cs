using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithExpirationService.LockStorage;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithExpirationService.QueueStorage;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithExpirationService.RentExtender;

namespace SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithExpirationService.LockCreatorStorage
{
    public class LockCreatorStorage : ILockCreatorStorage
    {
        public LockCreatorStorage(ILockStorage lockStorage, IQueueStorage queueStorage, IRentExtender rentExtender)
        {
            this.lockStorage = lockStorage;
            this.queueStorage = queueStorage;
            this.rentExtender = rentExtender;
        }

        public string AddThreadToQueue(string lockId, string threadId, long timestamp)
        {
            return queueStorage.Add(lockId, threadId, timestamp);
        }

        public string GetFirstInQueue(string lockId)
        {
            return queueStorage.GetFirstElement(lockId);
        }

        public void RemoveThreadFromQueue(string lockId, string rowName, string threadId, long timestamp)
        {
            queueStorage.Remove(lockId, rowName, threadId, timestamp);
        }

        public void AddThreadToLock(string lockId, string rowName, string threadId)
        {
            lockStorage.AddThreadToLock(lockId, rowName, threadId);
        }

        public int GetThreadsCountInLock(string lockId)
        {
            return lockStorage.GetLocksCount(lockId);
        }

        public void RemoveThreadFromLock(string lockId, string rowName, string threadId)
        {
            lockStorage.RemoveThreadFromLock(lockId, rowName, threadId);
        }

        public void ExtendQueueRent(string lockId, string rowName, string threadId, long timestamp)
        {
            rentExtender.ExtendQueueRent(lockId, rowName, threadId, timestamp);
        }

        public void ExtendLockRent(string lockId, string rowName, string threadId)
        {
            rentExtender.ExtendLockRent(lockId, rowName, threadId);
        }

        private readonly ILockStorage lockStorage;
        private readonly IQueueStorage queueStorage;
        private readonly IRentExtender rentExtender;
    }
}