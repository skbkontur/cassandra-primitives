using System;

namespace SKBKontur.Catalogue.CassandraPrimitives.Storages.PersistentStorages
{
    public interface IPersistentStorage<T, TId>
        where T : class
    {
        void Write(T obj, DateTime timestamp);
        void Write(T[] objects, DateTime timestamp);

        T TryRead(TId id);
        T[] TryRead(TId[] ids);

        T[] ReadQuiet(TId[] ids);

        void Delete(TId id, DateTime timestamp);
        void Delete(TId[] ids, DateTime timestamp);

        void Update(TId id, Action<T> updateAction, DateTime timestamp);

        TId[] GetIds(TId exclusiveStartId, int count);
    }
}