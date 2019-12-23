namespace SkbKontur.Cassandra.Primitives.EventLog.SpecificStorages
{
    internal interface IScopedCassandraObject
    {
        string ScopeId { get; }
        string Id { get; }
    }
}