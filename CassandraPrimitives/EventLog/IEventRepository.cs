using System;
using System.Collections.Generic;

using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Primitives;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog
{
    public interface IEventRepository : IDisposable
    {
        EventInfo AddEvent(string scopeId, object eventContent);
        EventInfo[] AddEvents(string scopeId, object[] eventContent);
        EventInfo[] AddEvents(KeyValuePair<string, object>[] eventContent);
        Event GetEvent(EventId eventId);
        IEnumerable<Event> GetEvents(EventInfo exclusiveEventInfo, string[] shards);
        IEnumerable<EventContainer> GetEventsWithUnstableZone(EventInfo exclusiveEventInfo, string[] shards);
        IEnumerable<EventContainer> GetEventsWithUnstableZone(EventInfo exclusiveEventInfo, string[] shards, out EventInfo newExclusiveEventInfoIfEmpty);
    }
}