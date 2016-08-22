﻿using System;
using System.IO;

using NUnit.Framework;

using SKBKontur.Cassandra.ClusterDeployment;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Logging;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Tests
{
    [SetUpFixture]
    public class StartSingleCassandraSetUp
    {
        [SetUp]
        public static void SetUp()
        {
            Log4NetConfiguration.InitializeOnce();
            Node = new CassandraNode(Path.Combine(FindCassandraTemplateDirectory(AppDomain.CurrentDomain.BaseDirectory), @"2.2"))
                {
                    Name = "node_at_9360",
                    JmxPort = 7399,
                    GossipPort = 7400,
                    RpcPort = 9360,
                    CqlPort = 9343,
                    DeployDirectory = Path.Combine(FindSolutionRootDirectory(), @"Cassandra2.2"),
                    ListenAddress = "127.0.0.1",
                    RpcAddress = "127.0.0.1",
                    SeedAddresses = new[] {"127.0.0.1"},
                    ClusterName = "test_cluster"
                };
            Node.Restart();
        }

        [TearDown]
        public static void TearDown()
        {
            Node.Stop();
        }

        internal static CassandraNode Node { get; private set; }

        private static string FindSolutionRootDirectory()
        {
            var currentDirectory = AppDomain.CurrentDomain.BaseDirectory;
            while(!File.Exists(Path.Combine(currentDirectory, "Common.DotSettings")))
            {
                if(Directory.GetParent(currentDirectory) == null)
                    throw new Exception(string.Format("Cannot find project root directory. Trying to find from: '{0}'", AppDomain.CurrentDomain.BaseDirectory));
                currentDirectory = Directory.GetParent(currentDirectory).FullName;
            }

            return currentDirectory;
        }

        private static string FindCassandraTemplateDirectory(string currentDir)
        {
            if(currentDir == null)
                throw new Exception("Невозможно найти каталог с Cassandra-шаблонами");
            var cassandraTemplateDirectory = Path.Combine(currentDir, cassandraTemplates);
            return Directory.Exists(cassandraTemplateDirectory) ? cassandraTemplateDirectory : FindCassandraTemplateDirectory(Path.GetDirectoryName(currentDir));
        }

        private const string cassandraTemplates = @"Assemblies\CassandraTemplates";
    }
}