namespace SKBKontur.Catalogue.CassandraPrimitives.Storages.PersistentStorages
{
    public interface ICassandraObjectIdConverter<T, TId>
    {
        TId GetId(T obj);
        void CheckObjectIdentity(T obj);

        string IdToRowKey(TId id);
        TId RowKeyToId(string rowKey);
    }
}