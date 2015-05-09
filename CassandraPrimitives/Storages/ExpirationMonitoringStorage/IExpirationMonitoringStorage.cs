namespace SKBKontur.Catalogue.CassandraPrimitives.Storages.ExpirationMonitoringStorage
{
    public interface IExpirationMonitoringStorage
    {
        void AddEntry(ExpiringObjectMeta meta, long ticks);
        void DeleteEntry(ExpiringObjectMeta meta, long ticks);
        ExpiringObjectMeta[] GetEntries(long fromTicks, long toTicks);
    }
}