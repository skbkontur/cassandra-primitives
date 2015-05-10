namespace SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.Core.LockStorage
{
    public interface ILockStorage
    {
        void AddThreadToLock(string lockId, string rowName, string threadId);
        void RemoveThreadFromLock(string lockId, string rowName, string threadId);
        int GetLocksCount(string lockId);
        void ExtendRent(string lockId, string rowName, string threadId);
    }
}