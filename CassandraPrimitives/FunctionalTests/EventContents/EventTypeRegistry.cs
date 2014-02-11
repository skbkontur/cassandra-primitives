using SKBKontur.Catalogue.CassandraPrimitives.EventLog.EventLog;
using SKBKontur.Catalogue.CassandraPrimitives.FunctionalTests.EventContents.Contents;

namespace SKBKontur.Catalogue.CassandraPrimitives.FunctionalTests.EventContents
{
    public class EventTypeRegistry : EventTypeRegistryBase
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