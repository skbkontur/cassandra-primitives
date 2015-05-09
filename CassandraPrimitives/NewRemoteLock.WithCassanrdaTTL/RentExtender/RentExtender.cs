﻿using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithCassanrdaTTL.LockStorage;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithCassanrdaTTL.QueueStorage;

namespace SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithCassanrdaTTL.RentExtender
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