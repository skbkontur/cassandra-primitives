namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.SchemeActualizer
{
    public interface ICassandraSchemeActualizer
    {
        void AddNewColumnFamilies();
        void TruncateAllColumnFamilies();
        void TruncateColumnFamily(string keyspace, string columnFamily);
        void DropDatabase();
    }
}