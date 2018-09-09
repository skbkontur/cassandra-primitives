using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Primitives;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.Implementation
{
    internal interface IEventLogger : IDisposable
    {
        EventInfo[] Write(params EventStorageElement[] events);
        Task<EventInfo[]> WriteAsync(params EventStorageElement[] events);
        IEnumerable<EventStorageElementContainer> ReadEventsWithUnstableZone(EventInfo startEventInfo, string[] shards, out EventInfo newExclusiveEventInfo);
    }
}