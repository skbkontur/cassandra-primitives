using SKBKontur.Catalogue.CassandraPrimitives.Storages.Primitives;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Settings
{
    public static class ColumnFamilies
    {
        public static ColumnFamilyFullName ticksHolder = new ColumnFamilyFullName("RemoteLockBenchmark", "TicksHolder");
        public static ColumnFamilyFullName eventLog = new ColumnFamilyFullName("RemoteLockBenchmark", "EventLog");
        public static ColumnFamilyFullName eventLogAdditionalInfo = new ColumnFamilyFullName("RemoteLockBenchmark", "EventLogAdditionalInfo");
        public static ColumnFamilyFullName remoteLock = new ColumnFamilyFullName("RemoteLockBenchmark", "RemoteLock");
    }
}