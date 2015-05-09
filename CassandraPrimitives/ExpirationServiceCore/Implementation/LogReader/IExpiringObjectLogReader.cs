using SKBKontur.Catalogue.CassandraPrimitives.Storages.ExpirationMonitoringStorage;

namespace SKBKontur.Catalogue.CassandraPrimitives.ExpirationServiceCore.Implementation.LogReader
{
    public interface IExpiringObjectLogReader
    {
        ExpiringObjectMeta[] GetNewMetas();
    }
}