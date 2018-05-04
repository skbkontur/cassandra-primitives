using System;
using System.IO;

using SkbKontur.Cassandra.Local;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.CassandraInitialisation
{
    public static class CassandraDeployer
    {
        public static void DeployCassandra(CassandraNodeSettings settings, string deployDirectory)
        {
            var node = CreateNodeBySettings(settings, deployDirectory);
            node.Deploy();
        }

        internal static LocalCassandraNode CreateNodeBySettings(CassandraNodeSettings settings, string deployDirectory)
        {
            var node = new LocalCassandraNode(Path.Combine(FindCassandraTemplateDirectory(AppDomain.CurrentDomain.BaseDirectory), "v2.2.x"), deployDirectory)
                {
                    LocalNodeName = settings.Name,
                    JmxPort = settings.JmxPort,
                    GossipPort = settings.GossipPort,
                    RpcPort = settings.RpcPort,
                    CqlPort = settings.CqlPort,
                    ListenAddress = settings.ListenAddress,
                    RpcAddress = settings.RpcAddress,
                    SeedAddresses = settings.SeedAddresses,
                    ClusterName = settings.ClusterName,
                    HeapSize = settings.HeapSize
                };
            return node;
        }

        private static string FindCassandraTemplateDirectory(string currentDir)
        {
            if (currentDir == null)
                throw new Exception("Can't find directory with Cassandra templates");
            var cassandraTemplateDirectory = Path.Combine(currentDir, cassandraTemplates);
            return Directory.Exists(cassandraTemplateDirectory) ? cassandraTemplateDirectory : FindCassandraTemplateDirectory(Path.GetDirectoryName(currentDir));
        }

        private const string cassandraTemplates = @"cassandra-local\cassandra";
    }
}