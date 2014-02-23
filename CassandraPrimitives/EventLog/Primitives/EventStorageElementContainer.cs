namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.Primitives
{
    internal class EventStorageElementContainer
    {
        public bool StableZone { get; set; }
        public EventStorageElement EventStorageElement { get; set; }
    }
}