using System;

namespace SkbKontur.Cassandra.Primitives.EventLog.Configuration.TypeIdentifiers
{
    public interface IEventTypeIdentifierProvider
    {
        Type GetTypeByIdentifier(string boxEventType);
        string GetIdentifierByType(Type eventContentType);
    }
}