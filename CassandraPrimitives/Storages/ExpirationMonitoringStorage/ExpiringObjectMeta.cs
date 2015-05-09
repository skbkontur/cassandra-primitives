namespace SKBKontur.Catalogue.CassandraPrimitives.Storages.ExpirationMonitoringStorage
{
    public class ExpiringObjectMeta
    {
        public string Keyspace { get; set; }
        public string ColumnFamily { get; set; }
        public string Row { get; set; }
        public string Column { get; set; }
    }
}