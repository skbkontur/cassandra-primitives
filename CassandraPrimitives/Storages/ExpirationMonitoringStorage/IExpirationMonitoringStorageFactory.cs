using SKBKontur.Catalogue.CassandraPrimitives.Storages.Primitives;

namespace SKBKontur.Catalogue.CassandraPrimitives.Storages.ExpirationMonitoringStorage
{
    public interface IExpirationMonitoringStorageFactory
    {
        IExpirationMonitoringStorage CreateStorage(ColumnFamilyFullName columnFamilyFullName);
    }
}