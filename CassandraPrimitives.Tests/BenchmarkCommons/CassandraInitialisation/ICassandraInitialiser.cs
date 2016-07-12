using System;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.CassandraInitialisation
{
    public interface ICassandraInitialiser : IDisposable
    {
        void CreateNode(CassandraNodeSettings settings);
        void StopAllNodes();
    }
}