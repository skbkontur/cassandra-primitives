namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.SpecificStorages
{
    public interface IScopedCassandraObject
    {
        string ScopeId { get; }
        string Id { get; }
    }
}