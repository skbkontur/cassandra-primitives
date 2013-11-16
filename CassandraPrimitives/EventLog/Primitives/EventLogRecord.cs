namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.Primitives
{
    public class EventLogRecord
    {
        public EventStorageElement StorageElement { get; set; }
        public bool IsBad { get; set; }
    }
}