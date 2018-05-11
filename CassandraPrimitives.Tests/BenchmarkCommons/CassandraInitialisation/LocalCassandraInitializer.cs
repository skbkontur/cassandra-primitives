using System;
using System.Collections.Generic;
using System.IO;

using SkbKontur.Cassandra.Local;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.CassandraInitialisation
{
    public class LocalCassandraInitializer : ICassandraInitialiser
    {
        public LocalCassandraInitializer()
        {
            cassandraNodes = new List<LocalCassandraNode>();
        }

        public void CreateNode(CassandraNodeSettings settings)
        {
            var node = CassandraDeployer.CreateNodeBySettings(settings, Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "..", "Cassandra2.2"));
            node.Restart();
            cassandraNodes.Add(node);
        }

        public void StopAllNodes()
        {
            foreach (var cassandraNode in cassandraNodes)
                cassandraNode.Stop();
        }

        public void Dispose()
        {
            StopAllNodes();
        }

        private readonly List<LocalCassandraNode> cassandraNodes;
    }
}