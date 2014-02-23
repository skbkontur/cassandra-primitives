using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Configuration.TypeIdentifiers;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.Commons.Contents;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.EventLoggerBenchmark.EventContents
{
    public class EventTypeRegistry : EventTypeRegistryBase
    {
        public EventTypeRegistry()
        {
            Register<TestContent>("TestContent");
        }
    }
}