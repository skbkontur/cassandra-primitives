using SKBKontur.Catalogue.CassandraPrimitives.Storages.Primitives;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.SchemeActualizer
{
    public interface ICassandraMetadataProvider
    {
        ColumnFamilyFullName[] GetColumnFamilies();
    }
}