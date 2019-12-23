using CassandraPrimitives.Tests.Commons.Contents;
using CassandraPrimitives.Tests.FunctionalTests.EventContents.Contents;

using SkbKontur.Cassandra.Primitives.EventLog.Configuration.TypeIdentifiers;

namespace CassandraPrimitives.Tests.FunctionalTests.EventContents
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