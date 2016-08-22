using System;
using System.Collections.Generic;
using System.IO;
using System.Numerics;

using SKBKontur.Cassandra.ClusterDeployment;

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

        private static string FindCassandraTemplateDirectory(string currentDir)
        {
            if (currentDir == null)
                throw new Exception("Can't find directory with Cassandra templates");
            var cassandraTemplateDirectory = Path.Combine(currentDir, cassandraTemplates);
            return Directory.Exists(cassandraTemplateDirectory) ? cassandraTemplateDirectory : FindCassandraTemplateDirectory(Path.GetDirectoryName(currentDir));
        }

        public static List<string> GenerateTokenRing(int nodesCount)
        {
            var result = new List<string>();
            for (var i = 0; i < nodesCount; i++)
            {
                var tokenIndex = i + 1;
                var bigInteger = new BigInteger(2);
                bigInteger = BigInteger.Pow(bigInteger, 127);
                bigInteger = tokenIndex * bigInteger / nodesCount;
                result.Add(bigInteger.ToString());
            }
            return result;
        }

        private const string cassandraTemplates = @"Assemblies\CassandraTemplates";
    }
}