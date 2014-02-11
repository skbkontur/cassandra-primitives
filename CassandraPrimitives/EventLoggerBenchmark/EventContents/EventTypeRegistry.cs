using SKBKontur.Catalogue.CassandraPrimitives.EventLog.EventLog;
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