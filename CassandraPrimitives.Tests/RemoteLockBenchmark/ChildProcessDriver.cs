using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.CassandraRemoteLock;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.ExternalLogging.HttpLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.TestConfigurations;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark
{
    public class ChildProcessDriver
    {
        public static void RunSingleTest(int processInd, TestConfiguration configuration, string workingDirectory)
        {
            ICassandraClusterSettings cassandraClusterSettings;
            using (var httpExternalDataProvider = new HttpExternalDataProvider())
                cassandraClusterSettings = httpExternalDataProvider.GetCassandraSettings().Result;
            using (var externalLogger = new HttpExternalLogger(processInd))
            using (var remoteLockGetter = new CassandraRemoteLockGetter(cassandraClusterSettings, externalLogger))
            {
                var test = new SimpleTest(configuration, processInd, remoteLockGetter);
                using (var testRunner = new TestRunner<SimpleTestResult>(configuration, externalLogger))
                    testRunner.RunTestAndPublishResults(test);
            }
        }
    }
}