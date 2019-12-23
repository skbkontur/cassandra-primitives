using System;
using System.Collections.Generic;

namespace SkbKontur.Cassandra.Primitives.EventLog.Configuration.TypeIdentifiers
{
    public abstract class EventTypeRegistryBase : IEventTypeIdentifierProvider
    {
        public Type GetTypeByIdentifier(string eventType)
        {
            if (!typeByEventType.ContainsKey(eventType))
                throw new Exception($"Unknown boxEventType {eventType}");
            return typeByEventType[eventType];
        }

        public string GetIdentifierByType(Type eventContentType)
        {
            if (!eventTypeByType.ContainsKey(eventContentType))
                throw new Exception($"Unknown eventContentType {eventContentType.FullName}");
            return eventTypeByType[eventContentType];
        }

        protected void Register<T>(string eventType)
        {
            typeByEventType.Add(eventType, typeof(T));
            eventTypeByType.Add(typeof(T), eventType);
        }

        private readonly Dictionary<string, Type> typeByEventType = new Dictionary<string, Type>();
        private readonly Dictionary<Type, string> eventTypeByType = new Dictionary<Type, string>();
    }
}