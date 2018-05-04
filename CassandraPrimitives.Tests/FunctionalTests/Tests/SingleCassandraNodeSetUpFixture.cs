using System;
using System.IO;

using NUnit.Framework;

using SKBKontur.Cassandra.ClusterDeployment;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.Commons.Logging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Logging;

using Vostok.Logging;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Tests
{
    [SetUpFixture]
    public class SingleCassandraNodeSetUpFixture
    {
        [SetUp]
        public static void SetUp()
        {
            Log4NetConfiguration.InitializeOnce();
            Node = new CassandraNode(
                Path.Combine(FindCassandraTemplateDirectory(AppDomain.CurrentDomain.BaseDirectory), @"2.2"),
                Path.Combine(FindSolutionRootDirectory(), @"DeployedCassandra"),
                logger
            );
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
        private static readonly ILog logger = new Log4NetWrapper(typeof(SingleCassandraNodeSetUpFixture));
    }
}