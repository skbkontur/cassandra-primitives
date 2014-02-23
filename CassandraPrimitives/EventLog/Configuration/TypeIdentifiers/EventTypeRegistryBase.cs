using System;
using System.Collections.Generic;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.Configuration.TypeIdentifiers
{
    public abstract class EventTypeRegistryBase : IEventTypeIdentifierProvider
    {
        public Type GetTypeByIdentifier(string eventType)
        {
            if(!typeByEventType.ContainsKey(eventType))
                throw new Exception(string.Format("Unknown boxEventType {0}", eventType));
            return typeByEventType[eventType];
        }

        public string GetIdentifierByType(Type eventContentType)
        {
            if(!eventTypeByType.ContainsKey(eventContentType))
                throw new Exception(string.Format("Unknown eventContentType {0}", eventContentType.FullName));
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