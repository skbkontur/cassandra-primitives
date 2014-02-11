using System;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.EventLog
{
    public interface IEventTypeIdentifierProvider
    {
        Type GetTypeByIdentifier(string boxEventType);
        string GetIdentifierByType(Type eventContentType);
    }
}