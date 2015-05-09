using SKBKontur.Catalogue.CassandraPrimitives.Storages.ExpirationMonitoringStorage;

namespace SKBKontur.Catalogue.CassandraPrimitives.ExpirationServiceCore.Implementation.ExpiryChecker
{
    public interface IExpiryChecker
    {
        void AddNewEntries(ExpiringObjectMeta[] metas);
        void Check();
    }
}