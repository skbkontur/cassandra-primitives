using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Primitives;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.PersistentStorages;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.EventLog
{
    internal interface IEventInfoRepository : IPersistentStorage<EventInfo, EventId>
    {
    }
}