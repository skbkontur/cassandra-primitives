using System.Collections.Generic;

using SKBKontur.Cassandra.ClusterDeployment;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.CassandraInitialisation
{
    public class LocalCassandraInitializer : ICassandraInitialiser
    {
        public LocalCassandraInitializer()
        {
            cassandraNodes = new List<CassandraNode>();
        }

        public void CreateNode(CassandraNodeSettings settings)
        {
            var node = CassandraDeployer.CreateNodeBySettings(settings);
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

        private readonly List<CassandraNode> cassandraNodes;
    }
}