namespace SkbKontur.Cassandra.Primitives.EventLog.Primitives
{
    internal class EventLogRecord
    {
        public EventStorageElement StorageElement { get; set; }
        public bool IsBad { get; set; }
    }
}