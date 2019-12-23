using SKBKontur.Catalogue.CassandraPrimitives.Storages.Primitives;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Settings
{
    public static class ColumnFamilies
    {
        public static ColumnFamilyFullName eventLog = new ColumnFamilyFullName("CassandraPrimitives", "EventLog");
        public static ColumnFamilyFullName eventLogAdditionalInfo = new ColumnFamilyFullName("CassandraPrimitives", "EventLogAdditionalInfo");
        public static ColumnFamilyFullName remoteLock = new ColumnFamilyFullName("CassandraPrimitives", "RemoteLock");
    }
}