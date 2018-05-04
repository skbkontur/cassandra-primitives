using System;
using System.IO;

using SKBKontur.Cassandra.ClusterDeployment;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.Commons.Logging;

using Vostok.Logging;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.CassandraInitialisation
{
    public static class CassandraDeployer
    {
        public static void DeployCassandra(CassandraNodeSettings settings, string deployDirectory)
        {
            var node = CreateNodeBySettings(settings, deployDirectory);
            node.Deploy();
        }

        internal static CassandraNode CreateNodeBySettings(CassandraNodeSettings settings, string deployDirectory)
        {
            var node = new CassandraNode(Path.Combine(FindCassandraTemplateDirectory(AppDomain.CurrentDomain.BaseDirectory), "2.2"), deployDirectory, logger)
                {
                    Name = settings.Name,
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

        private const string cassandraTemplates = @"Assemblies\CassandraTemplates";
        private static readonly ILog logger = new Log4NetWrapper(typeof(CassandraDeployer));
    }
}