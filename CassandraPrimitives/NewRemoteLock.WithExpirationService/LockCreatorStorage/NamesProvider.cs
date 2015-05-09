namespace SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithExpirationService.LockCreatorStorage
{
    public static class NamesProvider
    {
        public static string GetQueueRowName(string rowName)
        {
            return rowName;
        }

        public static string GetQueueColumnName(string threadId, long timestamp)
        {
            return string.Format("{0:D20}_{1}", timestamp, threadId);
        }

        public static string GetLockRowName(string rowName)
        {
            return string.Format("Lock_{0}", rowName);
        }

        public static string GetLockColumnName(string threadId)
        {
            return threadId;
        }
    }
}