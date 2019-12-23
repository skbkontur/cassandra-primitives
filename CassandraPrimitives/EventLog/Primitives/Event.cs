namespace SkbKontur.Cassandra.Primitives.EventLog.Primitives
{
    public class Event
    {
        public EventInfo EventInfo { get; set; }
        public object EventContent { get; set; }
    }
}