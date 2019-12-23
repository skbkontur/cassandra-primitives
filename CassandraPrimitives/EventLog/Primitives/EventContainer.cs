namespace SkbKontur.Cassandra.Primitives.EventLog.Primitives
{
    public class EventContainer
    {
        public bool StableZone { get; set; }
        public Event Event { get; set; }
    }
}