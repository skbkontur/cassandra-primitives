namespace SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.Core.MetaStorage
{
    public interface IMetaStorage
    {
        LockMeta[] GetMetas(string lockId);
        string GetActualRowKey(string lockId);
        void DeleteMeta(string lockId, LockMeta meta);
    }
}