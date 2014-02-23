using System;
using System.Collections.Generic;
using System.Linq;

using GroBuf;

using MoreLinq;

using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Configuration.TypeIdentifiers;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Primitives;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Sharding;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.Implementation
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

        public EventInfo[] AddEvents(KeyValuePair<string, object>[] events)
        {
            var storageElements = events.Select(
                ev =>
                {
                    var eventId = new EventId { ScopeId = ev.Key, Id = Guid.NewGuid().ToString() };
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
            return eventLogger.Write(storageElements);
        }

        public Event GetEvent(EventId eventId)
        {
            var storageElement = eventLogger.ReadEvent(eventId);
            return FromStorageElement(storageElement);
        }

        public void Dispose()
        {
            eventLogger.Dispose();
        }

        public IEnumerable<Event> GetEvents(EventInfo exclusiveEventInfo, string[] shards)
        {
            var events = GetEventsWithUnstableZone(exclusiveEventInfo, shards);
            foreach(var ev in events)
            {
                if(ev.StableZone) yield return ev.Event;
                else yield break;
            }
        }

        public IEnumerable<EventContainer> GetEventsWithUnstableZone(EventInfo exclusiveEventInfo, string[] shards)
        {
            EventInfo newExclusiveEventInfo;
            return GetEventsWithUnstableZone(exclusiveEventInfo, shards, out newExclusiveEventInfo);
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

        private readonly IEventTypeIdentifierProvider eventTypeIdentifierProvider;
        private readonly IEventLogger eventLogger;
        private readonly IShardCalculator shardCalculator;
        private readonly ISerializer serializer;

        private const int batchSize = 1000;
    }
}