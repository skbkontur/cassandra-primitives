using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using SkbKontur.Cassandra.Primitives.EventLog.Primitives;

namespace SkbKontur.Cassandra.Primitives.EventLog.Implementation
{
    internal interface IEventLogger : IDisposable
    {
        EventInfo[] Write(params EventStorageElement[] events);
        Task<EventInfo[]> WriteAsync(params EventStorageElement[] events);
        IEnumerable<EventStorageElementContainer> ReadEventsWithUnstableZone(EventInfo startEventInfo, string[] shards, out EventInfo newExclusiveEventInfo);
    }
}