namespace SkbKontur.Cassandra.Primitives.Storages.PersistentStorages
{
    public static class IdConverterExtensions
    {
        public static string IdToRowKeyDef<T, TId>(this ICassandraObjectIdConverter<T, TId> cassandraObjectIdConverter, TId id, string defaultValue = null)
        {
            return id == null ? defaultValue : cassandraObjectIdConverter.IdToRowKey(id);
        }

        public static string GetRowKeyDef<T, TId>(this ICassandraObjectIdConverter<T, TId> cassandraObjectIdConverter, T obj, string defaultValue = null)
        {
            var id = cassandraObjectIdConverter.GetId(obj);
            return cassandraObjectIdConverter.IdToRowKeyDef(id, defaultValue);
        }

        public static string GetRowKey<T, TId>(this ICassandraObjectIdConverter<T, TId> cassandraObjectIdConverter, T obj)
        {
            var id = cassandraObjectIdConverter.GetId(obj);
            return cassandraObjectIdConverter.IdToRowKey(id);
        }
    }
}