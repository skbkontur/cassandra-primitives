using System;
using System.Collections.Generic;

using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Primitives;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.EventLog
{
    public interface IEventLogger : IDisposable
    {
        EventInfo[] Write(params EventStorageElement[] events);
        EventStorageElement ReadEvent(EventId eventId);
        IEnumerable<EventStorageElementContainer> ReadEventsWithUnstableZone(EventInfo startEventInfo, string[] shards, out EventInfo newExclusiveEventInfo);
    }
}