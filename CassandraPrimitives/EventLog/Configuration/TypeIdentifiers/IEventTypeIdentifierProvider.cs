using System;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.Configuration.TypeIdentifiers
{
    public interface IEventTypeIdentifierProvider
    {
        Type GetTypeByIdentifier(string boxEventType);
        string GetIdentifierByType(Type eventContentType);
    }
}