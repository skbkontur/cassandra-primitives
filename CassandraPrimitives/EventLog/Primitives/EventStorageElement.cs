namespace SkbKontur.Cassandra.Primitives.EventLog.Primitives
{
    internal class EventStorageElement
    {
        public EventInfo EventInfo { get; set; }
        public string EventType { get; set; }
        public byte[] EventContent { get; set; }
    }
}