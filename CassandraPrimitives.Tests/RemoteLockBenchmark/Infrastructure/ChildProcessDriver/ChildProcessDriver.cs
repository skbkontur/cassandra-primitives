using System;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.ExternalLogging.HttpLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.TestConfigurations;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.TestRunning;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.ProgressMessages;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.Tests;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.ChildProcessDriver
{
    public static class ChildProcessDriver
    {
        public static void RunSingleTest(TestConfiguration configuration, int processInd, string processToken)
        {
            switch (configuration.TestScenario)
            {
            case TestScenarios.Timeline:
                RunTimelineTest(configuration, processInd, processToken);
                break;
            case TestScenarios.WaitForLock:
                RunWaitForLockTest(configuration, processInd, processToken);
                break;
            case TestScenarios.SeriesOfLocks:
                RunSeriesOfLocksTest(configuration, processInd, processToken);
                break;
            default:
                throw new Exception(string.Format("Unknown TestScenario {0}", configuration.TestScenario));
            }
        }

        private static void RunTimelineTest(TestConfiguration configuration, int processInd, string processToken)
        {
            using (var externalLogger = new HttpExternalLogger<TimelineProgressMessage>(processInd, configuration.RemoteHostName, processToken))
            using (var httpExternalDataGetter = new HttpExternalDataGetter(configuration.RemoteHostName, configuration.HttpPort))
            {
                var remoteLockGetterProvider = new RemoteLockGetterProvider(httpExternalDataGetter, configuration, externalLogger);
                var test = new TimelineTest(configuration, remoteLockGetterProvider, externalLogger, httpExternalDataGetter, processInd);
                using (var testRunner = new TestRunner<TimelineProgressMessage>(configuration, externalLogger))
                    testRunner.RunTestAndPublishResults(test);
            }
        }

        private static void RunWaitForLockTest(TestConfiguration configuration, int processInd, string processToken)
        {
            using (var externalLogger = new HttpExternalLogger<WaitForLockProgressMessage>(processInd, configuration.RemoteHostName, processToken))
            using (var httpExternalDataGetter = new HttpExternalDataGetter(configuration.RemoteHostName, configuration.HttpPort))
            {
                var remoteLockGetterProvider = new RemoteLockGetterProvider(httpExternalDataGetter, configuration, externalLogger);
                var test = new WaitForLockTest(configuration, remoteLockGetterProvider, externalLogger, httpExternalDataGetter);
                using (var testRunner = new TestRunner<WaitForLockProgressMessage>(configuration, externalLogger))
                    testRunner.RunTestAndPublishResults(test);
            }
        }

        private static void RunSeriesOfLocksTest(TestConfiguration configuration, int processInd, string processToken)
        {
            using (var externalLogger = new HttpExternalLogger<SeriesOfLocksProgressMessage>(processInd, configuration.RemoteHostName, processToken))
            using (var httpExternalDataGetter = new HttpExternalDataGetter(configuration.RemoteHostName, configuration.HttpPort))
            {
                var remoteLockGetterProvider = new RemoteLockGetterProvider(httpExternalDataGetter, configuration, externalLogger);
                var test = new SeriesOfLocksTest(configuration, remoteLockGetterProvider, externalLogger, httpExternalDataGetter);
                using (var testRunner = new TestRunner<SeriesOfLocksProgressMessage>(configuration, externalLogger))
                    testRunner.RunTestAndPublishResults(test);
            }
        }
    }
}