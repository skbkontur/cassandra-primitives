using SKBKontur.Catalogue.CassandraPrimitives.Storages.Primitives;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Implementations.CassandraSettings
{
    public static class ColumnFamilies
    {
        public static ColumnFamilyFullName remoteLock = new ColumnFamilyFullName("RemoteLockBenchmark", "RemoteLock");
    }
}