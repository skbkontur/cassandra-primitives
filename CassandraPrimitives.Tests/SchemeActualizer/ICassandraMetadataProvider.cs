using SkbKontur.Cassandra.Primitives.Storages.Primitives;

namespace CassandraPrimitives.Tests.SchemeActualizer
{
    public interface ICassandraMetadataProvider
    {
        ColumnFamilyFullName[] GetColumnFamilies();
    }
}