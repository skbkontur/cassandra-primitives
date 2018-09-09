using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Primitives;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog
{
    public interface IEventRepository : IDisposable
    {
        EventInfo AddEvent(string scopeId, object eventContent);
        EventInfo[] AddEvents(string scopeId, object[] eventContent);
        EventInfo[] AddEvents(KeyValuePair<string, object>[] eventContent);
        Task<EventInfo[]> AddEventsAsync(KeyValuePair<string, object>[] events);
        IEnumerable<Event> GetEvents(EventInfo exclusiveEventInfo, string[] shards);
        IEnumerable<Event> GetEvents(EventInfo exclusiveEventInfo, string[] shards, out EventInfo newExclusiveEventInfoIfEmpty);
        IEnumerable<EventContainer> GetEventsWithUnstableZone(EventInfo exclusiveEventInfo, string[] shards);
        IEnumerable<EventContainer> GetEventsWithUnstableZone(EventInfo exclusiveEventInfo, string[] shards, out EventInfo newExclusiveEventInfoIfEmpty);
    }
}