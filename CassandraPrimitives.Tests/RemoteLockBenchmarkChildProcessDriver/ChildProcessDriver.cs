using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkChildProcessDriver.ExternalLogging.Http;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkChildProcessDriver.RemoteLocks;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.TestConfigurations;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkChildProcessDriver
{
    public class ChildProcessDriver
    {
        public static void RunSingleTest(int processInd, TestConfiguration configuration, string workingDirectory, string processToken)
        {
            ICassandraClusterSettings cassandraClusterSettings;
            using (var httpExternalDataProvider = new HttpExternalDataGetter(configuration.remoteHostName))
                cassandraClusterSettings = httpExternalDataProvider.GetCassandraSettings().Result;
            using (var externalLogger = new HttpExternalLogger(processInd, configuration.remoteHostName, processToken))
            using (var remoteLockGetter = new CassandraRemoteLockGetter(cassandraClusterSettings, externalLogger))
            {
                var test = new SimpleTest(configuration, remoteLockGetter, externalLogger);
                using (var testRunner = new TestRunning.TestRunner<SimpleProgressMessage>(configuration, externalLogger))
                    testRunner.RunTestAndPublishResults(test);
            }
        }
    }
}