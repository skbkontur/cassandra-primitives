namespace SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithCassanrdaTTL.LockCreatorStorage
{
    public interface ILockCreatorStorage
    {
        string AddThreadToQueue(string lockId, string threadId, long timestamp);
        string GetFirstInQueue(string lockId);
        void RemoveThreadFromQueue(string lockId, string rowName, string threadId, long timestamp);

        void AddThreadToLock(string lockId, string rowName, string threadId);
        int GetThreadsCountInLock(string lockId);
        void RemoveThreadFromLock(string lockId, string rowName, string threadId);

        void ExtendQueueRent(string lockId, string rowName, string threadId, long timestamp);
        void ExtendLockRent(string lockId, string rowName, string threadId);
    }
}