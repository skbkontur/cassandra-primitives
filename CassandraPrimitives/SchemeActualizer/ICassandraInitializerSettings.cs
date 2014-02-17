namespace SKBKontur.Catalogue.CassandraPrimitives.SchemeActualizer
{
    public interface ICassandraInitializerSettings
    {
        int RowCacheSize { get; }
        int ReplicationFactor { get; }
    } 
}