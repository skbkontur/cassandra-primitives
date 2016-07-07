using System;
using System.IO;

using SKBKontur.Cassandra.ClusterDeployment;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons
{
    public class CassandraInitializer
    {
        private const string cassandraTemplates = @"Assemblies\CassandraTemplates";

        public static CassandraNode CreateCassandraNode()
        {
            return new CassandraNode(Path.Combine(FindCassandraTemplateDirectory(AppDomain.CurrentDomain.BaseDirectory), @"1.2"))
                {
                    Name = "node_at_9360",
                    JmxPort = 7399,
                    GossipPort = 7400,
                    RpcPort = 9360,
                    CqlPort = 9343,
                    DataBaseDirectory = @"../data/",
                    DeployDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @"..\Cassandra1.2"),
                    ListenAddress = "127.0.0.1",
                    RpsAddress = "0.0.0.0",
                    SeedAddresses = new[] {"127.0.0.1"},
                    InitialToken = "",
                    ClusterName = "test_cluster"
                };
        }

        private static string FindCassandraTemplateDirectory(string currentDir)
        {
            if (currentDir == null)
                throw new Exception("Can't find directory with Cassandra templates");
            var cassandraTemplateDirectory = Path.Combine(currentDir, cassandraTemplates);
            return Directory.Exists(cassandraTemplateDirectory) ? cassandraTemplateDirectory : FindCassandraTemplateDirectory(Path.GetDirectoryName(currentDir));
        }
    }
}