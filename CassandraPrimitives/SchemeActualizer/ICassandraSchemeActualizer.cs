namespace SKBKontur.Catalogue.CassandraPrimitives.SchemeActualizer
{
    public interface ICassandraSchemeActualizer
    {
        void AddNewColumnFamilies();
        void TruncateAllColumnFamilies();
        void TruncateColumnFamily(string keyspace, string columnFamily);
        void DropDatabase();
    }
}