namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.SpecificStorages
{
    internal interface IScopedCassandraObject
    {
        string ScopeId { get; }
        string Id { get; }
    }
}