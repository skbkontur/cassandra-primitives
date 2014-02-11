namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.Primitives
{
    public class QueueEntry
    {
        public EventStorageElement[] events;
        public DeferredResult result;
        public int priority;
    }
}