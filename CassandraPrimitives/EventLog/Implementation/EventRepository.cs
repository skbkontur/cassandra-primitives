using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

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
            EventInfo newExclusiveEventInfo;
            return GetEvents(exclusiveEventInfo, shards, out newExclusiveEventInfo);
        }

        public IEnumerable<Event> GetEvents(EventInfo exclusiveEventInfo, string[] shards, out EventInfo newExclusiveEventInfoIfEmpty)
        {
            var rawEvents = GetEventsWithUnstableZone(exclusiveEventInfo, shards, out newExclusiveEventInfoIfEmpty);
            var events = rawEvents.TakeWhile(x => x.StableZone).Select(x => x.Event).ToArray();

            if(events.Length != 0)
            {
                newExclusiveEventInfoIfEmpty = events.Last().EventInfo;
            }
            else if(rawEvents.Any())
            {
                var firstUnstableEventInfo = rawEvents.First().Event.EventInfo;
                if(firstUnstableEventInfo.Ticks - exclusiveEventInfo.Ticks <= 1)
                {
                    newExclusiveEventInfoIfEmpty = exclusiveEventInfo;
                }
                else
                {
                    newExclusiveEventInfoIfEmpty = new EventInfo { Ticks = firstUnstableEventInfo.Ticks - 1, Id = new EventId()};
                }
            }

            return events;
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