using SKBKontur.Catalogue.CassandraPrimitives.EventLog;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Configuration.TypeIdentifiers;
using SKBKontur.Catalogue.CassandraPrimitives.FunctionalTests.EventContents.Contents;

namespace SKBKontur.Catalogue.CassandraPrimitives.FunctionalTests.EventContents
{
    internal class EventTypeRegistry : EventTypeRegistryBase
    {
        public EventTypeRegistry()
        {
            Register<InboxRawMessageEventContent>(BoxEventType.InboxRawMessage);
            Register<OutboxRawMessageEventContent>(BoxEventType.OutboxRawMessage);
            Register<SentEventContent>(BoxEventType.Sent);
            Register<TestContent>("TestContent");
        }
    }
}