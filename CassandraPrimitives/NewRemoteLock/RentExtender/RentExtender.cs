using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.LockStorage;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.QueueStorage;

namespace SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.RentExtender
{
    public class RentExtender : IRentExtender
    {
        public RentExtender(IQueueStorage queueStorage, ILockStorage lockStorage)
        {
            this.queueStorage = queueStorage;
            this.lockStorage = lockStorage;
        }

        public void ExtendLockRent(string lockId, string rowName, string threadId)
        {
            lockStorage.ExtendRent(lockId, rowName, threadId);
        }

        public void ExtendQueueRent(string lockId, string rowName, string threadId, long timestamp)
        {
            queueStorage.ExtendRent(lockId, rowName, threadId, timestamp);
        }

        private readonly IQueueStorage queueStorage;
        private readonly ILockStorage lockStorage;
    }
}