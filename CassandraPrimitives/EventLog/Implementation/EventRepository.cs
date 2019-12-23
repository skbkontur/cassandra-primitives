using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using GroBuf;

using MoreLinq;

using SkbKontur.Cassandra.Primitives.EventLog.Configuration.TypeIdentifiers;
using SkbKontur.Cassandra.Primitives.EventLog.Primitives;
using SkbKontur.Cassandra.Primitives.EventLog.Sharding;

namespace SkbKontur.Cassandra.Primitives.EventLog.Implementation
{
    internal class EventRepository : IEventRepository
    {
        public EventRepository(
            IEventTypeIdentifierProvider eventTypeIdentifierProvider,
            IEventLogger eventLogger,
            IShardCalculator shardCalculator,
            ISerializer serializer)
        {
            this.eventTypeIdentifierProvider = eventTypeIdentifierProvider;
            this.eventLogger = eventLogger;
            this.shardCalculator = shardCalculator;
            this.serializer = serializer;
        }

        public EventInfo AddEvent(string scopeId, object eventContent)
        {
            var infos = AddEvents(scopeId, new[] {eventContent});
            return infos[0];
        }

        public EventInfo[] AddEvents(string scopeId, object[] eventContents)
        {
            return AddEvents(eventContents.Select(x => new KeyValuePair<string, object>(scopeId, x)).ToArray());
        }

        public async Task<EventInfo[]> AddEventsAsync(KeyValuePair<string, object>[] events)
        {
            var storageElements = SelectStorageElements(events);
            return await eventLogger.WriteAsync(storageElements).ConfigureAwait(false);
        }

        public EventInfo[] AddEvents(KeyValuePair<string, object>[] events)
        {
            var storageElements = SelectStorageElements(events);
            return eventLogger.Write(storageElements);
        }

        public void Dispose()
        {
            eventLogger.Dispose();
        }

        public IEnumerable<Event> GetEvents(EventInfo exclusiveEventInfo, string[] shards)
        {
            return GetEvents(exclusiveEventInfo, shards, out var newExclusiveEventInfo);
        }

        public IEnumerable<Event> GetEvents(EventInfo exclusiveEventInfo, string[] shards, out EventInfo newExclusiveEventInfoIfEmpty)
        {
            var events = GetEventsWithUnstableZone(exclusiveEventInfo, shards, out newExclusiveEventInfoIfEmpty);
            return events.TakeWhile(x => x.StableZone).Select(x => x.Event);
        }

        public IEnumerable<EventContainer> GetEventsWithUnstableZone(EventInfo exclusiveEventInfo, string[] shards)
        {
            return GetEventsWithUnstableZone(exclusiveEventInfo, shards, out var newExclusiveEventInfo);
        }

        public IEnumerable<EventContainer> GetEventsWithUnstableZone(EventInfo exclusiveEventInfo, string[] shards, out EventInfo newExclusiveEventInfoIfEmpty)
        {
            var allEvents = eventLogger.ReadEventsWithUnstableZone(exclusiveEventInfo, shards, out newExclusiveEventInfoIfEmpty);
            return allEvents.Batch(batchSize).SelectMany(elements => elements, (elements, element) => new EventContainer
                {
                    Event = FromStorageElement(element.EventStorageElement),
                    StableZone = element.StableZone
                });
        }

        private EventStorageElement[] SelectStorageElements(KeyValuePair<string, object>[] events)
        {
            var storageElements = events.Select(
                ev =>
                    {
                        var eventId = new EventId {ScopeId = ev.Key, Id = Guid.NewGuid().ToString()};
                        return new EventStorageElement
                            {
                                EventInfo = new EventInfo
                                    {
                                        Id = eventId,
                                        Shard = shardCalculator.CalculateShard(eventId, ev.Value)
                                    },
                                EventContent = serializer.Serialize(ev.Value.GetType(), ev.Value),
                                EventType = eventTypeIdentifierProvider.GetIdentifierByType(ev.Value.GetType())
                            };
                    }).ToArray();
            return storageElements;
        }

        private Event FromStorageElement(EventStorageElement storageElement)
        {
            var eventContentType = eventTypeIdentifierProvider.GetTypeByIdentifier(storageElement.EventType);
            var eventContent = serializer.Deserialize(eventContentType, storageElement.EventContent);
            return new Event
                {
                    EventInfo = storageElement.EventInfo,
                    EventContent = eventContent,
                };
        }

        private const int batchSize = 1000;

        private readonly IEventTypeIdentifierProvider eventTypeIdentifierProvider;
        private readonly IEventLogger eventLogger;
        private readonly IShardCalculator shardCalculator;
        private readonly ISerializer serializer;
    }
}