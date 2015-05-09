namespace SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithExpirationService.MetaStorage
{
    public interface IMetaStorage
    {
        LockMeta[] GetMetas(string lockId);
        string GetActualRowKey(string lockId);
        void DeleteMeta(string lockId, LockMeta meta);
    }
}