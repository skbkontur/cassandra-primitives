using System;
using System.IO;

using SKBKontur.Cassandra.ClusterDeployment;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.CassandraInitialisation
{
    public class CassandraDeployer
    {
        public static void DeployCassandra(CassandraNodeSettings settings, string deployDirectory)
        {
            var node = CreateNodeBySettings(settings, deployDirectory);
            node.Deploy();
        }

        internal static CassandraNode CreateNodeBySettings(CassandraNodeSettings settings, string deployDirectory)
        {
            var node = new CassandraNode(Path.Combine(FindCassandraTemplateDirectory(AppDomain.CurrentDomain.BaseDirectory), @"1.2"))
                {
                    Name = settings.Name,
                    JmxPort = settings.JmxPort,
                    GossipPort = settings.GossipPort,
                    RpcPort = settings.RpcPort,
                    CqlPort = settings.CqlPort,
                    DataBaseDirectory = settings.DataBaseDirectory,
                    DeployDirectory = deployDirectory,
                    ListenAddress = settings.ListenAddress,
                    RpsAddress = settings.RpsAddress,
                    SeedAddresses = settings.SeedAddresses,
                    InitialToken = settings.InitialToken,
                    ClusterName = settings.ClusterName,
                };
            return node;
        }

        internal static string FindCassandraTemplateDirectory(string currentDir)
        {
            if (currentDir == null)
                throw new Exception("Can't find directory with Cassandra templates");
            var cassandraTemplateDirectory = Path.Combine(currentDir, cassandraTemplates);
            return Directory.Exists(cassandraTemplateDirectory) ? cassandraTemplateDirectory : FindCassandraTemplateDirectory(Path.GetDirectoryName(currentDir));
        }

        private const string cassandraTemplates = @"Assemblies\CassandraTemplates";
    }
}