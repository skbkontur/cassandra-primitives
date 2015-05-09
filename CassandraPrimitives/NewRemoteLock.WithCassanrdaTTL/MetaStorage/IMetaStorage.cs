namespace SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithCassanrdaTTL.MetaStorage
{
    public interface IMetaStorage
    {
        LockMeta[] GetMetas(string lockId);
        string GetActualRowKey(string lockId);
        void DeleteMeta(string lockId, LockMeta meta);
    }
}