using System;
using System.IO;

using NUnit.Framework;

using SkbKontur.Cassandra.Local;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Logging;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Tests
{
    [SetUpFixture]
    public class SingleCassandraNodeSetUpFixture
    {
        [SetUp]
        public static void SetUp()
        {
            Log4NetConfiguration.InitializeOnce();
            var templateDirectory = Path.Combine(FindCassandraTemplateDirectory(AppDomain.CurrentDomain.BaseDirectory), @"v3.11.x");
            var deployDirectory = Path.Combine(FindSolutionRootDirectory(), @"DeployedCassandra");
            Node = new LocalCassandraNode(templateDirectory, deployDirectory)
                {
                    RpcPort = 9360,
                    CqlPort = 9343,
                    JmxPort = 7399,
                    GossipPort = 7400,
                };
            Node.Restart();
        }

        [TearDown]
        public static void TearDown()
        {
            Node.Stop();
        }

        internal static LocalCassandraNode Node { get; private set; }

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

        private const string cassandraTemplates = @"cassandra-local\cassandra";
    }
}