using SKBKontur.Catalogue.CassandraPrimitives.EventLog;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Configuration.TypeIdentifiers;
using SKBKontur.Catalogue.CassandraPrimitives.FunctionalTests.EventContents.Contents;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLoggerBenchmark.EventContents
{
    public class EventTypeRegistry : EventTypeRegistryBase
    {
        public EventTypeRegistry()
        {
            Register<TestContent>("TestContent");
        }
    }
}