namespace CassandraPrimitives.Tests.SchemeActualizer
{
    public interface ICassandraInitializerSettings
    {
        int RowCacheSize { get; }
        int ReplicationFactor { get; }
    }
}