namespace SkbKontur.Cassandra.Primitives.EventLog.SpecificStorages
{
    internal class ScopedCassandraObjectId
    {
        public string ScopeId { get; set; }
        public string Id { get; set; }
    }
}