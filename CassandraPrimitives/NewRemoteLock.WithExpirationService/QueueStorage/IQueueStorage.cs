namespace SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithExpirationService.QueueStorage
{
    public interface IQueueStorage
    {
        string Add(string lockId, string threadId, long timestamp);
        void Remove(string lockId, string rowName, string threadId, long timestamp);
        string GetFirstElement(string lockId);

        void ExtendRent(string lockId, string rowName, string threadId, long timestamp);
    }
}