using SKBKontur.Catalogue.CassandraPrimitives.Storages.Primitives;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.EventLoggerBenchmark.Settings
{
    public static class ColumnFamilies
    {
        public static ColumnFamilyFullName ticksHolder = new ColumnFamilyFullName("EventLoggerBenchmark", "TicksHolder");
        public static ColumnFamilyFullName eventLog = new ColumnFamilyFullName("EventLoggerBenchmark", "EventLog");
        public static ColumnFamilyFullName eventLogAdditionalInfo = new ColumnFamilyFullName("EventLoggerBenchmark", "EventLogAdditionalInfo");
        public static ColumnFamilyFullName remoteLock = new ColumnFamilyFullName("EventLoggerBenchmark", "RemoteLock");
    }
}