namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.Primitives
{
    internal class QueueEntry
    {
        public EventStorageElement[] events;
        public DeferredResult result;
        public int priority;
    }
}