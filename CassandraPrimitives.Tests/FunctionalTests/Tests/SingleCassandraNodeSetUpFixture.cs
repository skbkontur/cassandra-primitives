using System;
using System.IO;

using NUnit.Framework;

using SkbKontur.Cassandra.Local;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Tests
{
    [SetUpFixture]
    public class SingleCassandraNodeSetUpFixture
    {
        [OneTimeSetUp]
        public static void SetUp()
        {
            var templateDirectory = Path.Combine(FindCassandraTemplateDirectory(AppDomain.CurrentDomain.BaseDirectory), @"v3.11.x");
            var deployDirectory = Path.Combine(Path.GetTempPath(), "deployed_cassandra_v3.11.x");
            Node = new LocalCassandraNode(templateDirectory, deployDirectory)
                {
                    RpcPort = 9360,
                    CqlPort = 9343,
                    JmxPort = 7399,
                    GossipPort = 7400,
                };
            Node.Restart();
        }

        [OneTimeTearDown]
        public static void TearDown()
        {
            Node.Stop();
        }

        internal static LocalCassandraNode Node { get; private set; }

        private static string FindCassandraTemplateDirectory(string currentDir)
        {
            if (currentDir == null)
                throw new Exception("Невозможно найти каталог с Cassandra-шаблонами");
            var cassandraTemplateDirectory = Path.Combine(currentDir, cassandraTemplates);
            return Directory.Exists(cassandraTemplateDirectory) ? cassandraTemplateDirectory : FindCassandraTemplateDirectory(Path.GetDirectoryName(currentDir));
        }

        private const string cassandraTemplates = @"cassandra-local\cassandra";
    }
}