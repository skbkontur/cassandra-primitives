using System;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkChildProcessDriver.ExternalLogging.Http;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkChildProcessDriver.RemoteLocks;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkChildProcessDriver.TestRunning;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.TestConfigurations;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkChildProcessDriver
{
    public class ChildProcessDriver
    {
        public static void RunSingleTest(int processInd, TestConfiguration configuration, string workingDirectory, string processToken)
        {
            ICassandraClusterSettings cassandraClusterSettings;
            long timeCorrectionDelta;
            string lockId;
            using (var httpExternalDataGetter = new HttpExternalDataGetter(configuration.remoteHostName))
            {
                cassandraClusterSettings = httpExternalDataGetter.GetCassandraSettings().Result;
                timeCorrectionDelta = httpExternalDataGetter.GetTime().Result - (long)DateTime.UtcNow.Subtract(new DateTime(1970, 1, 1, 0, 0, 0)).TotalMilliseconds;
                lockId = httpExternalDataGetter.GetLockId().Result;
            }
            using (var externalLogger = new HttpExternalLogger<TimelineProgressMessage>(processInd, configuration.remoteHostName, processToken))
            using (var remoteLockGetter = new CassandraRemoteLockGetter(cassandraClusterSettings, externalLogger))
            {
                var test = new TimelineTest(configuration, remoteLockGetter, externalLogger, timeCorrectionDelta, lockId, processInd);
                using (var testRunner = new TestRunner<TimelineProgressMessage>(configuration, externalLogger))
                    testRunner.RunTestAndPublishResults(test);
            }
        }
    }
}