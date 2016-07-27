using System;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.ExternalLogging.HttpLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.TestConfigurations;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.TestRunning;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.ProgressMessages;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.Tests;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.ChildProcessDriver
{
    public class ChildProcessDriver
    {
        public static void RunSingleTest(TestConfiguration configuration, int processInd, string processToken, Type testType)
        {
            if (testType == typeof(TimelineTest))
                RunTimelineTest(configuration, processInd, processToken);
            else if (testType == typeof(TimelineTest))
                RunSimpleTest(configuration, processInd, processToken);
            else
                throw new Exception(String.Format("Unknown ITest implementation {0}", testType));
        }

        private static void RunTimelineTest(TestConfiguration configuration, int processInd, string processToken)
        {
            using (var externalLogger = new HttpExternalLogger<TimelineProgressMessage>(processInd, configuration.RemoteHostName, processToken))
            using (var httpExternalDataGetter = new HttpExternalDataGetter(configuration.RemoteHostName, configuration.HttpPort))
            {
                var remoteLockGetterProvider = new RemoteLockGetterProvider(httpExternalDataGetter, configuration, externalLogger);
                var test = new TimelineTest(configuration, remoteLockGetterProvider.RemoteLockGetter, externalLogger, httpExternalDataGetter, processInd);
                using (var testRunner = new TestRunner<TimelineProgressMessage>(configuration, externalLogger))
                    testRunner.RunTestAndPublishResults(test);
            }
        }

        private static void RunSimpleTest(TestConfiguration configuration, int processInd, string processToken)
        {
            using (var externalLogger = new HttpExternalLogger<SimpleProgressMessage>(processInd, configuration.RemoteHostName, processToken))
            using (var httpExternalDataGetter = new HttpExternalDataGetter(configuration.RemoteHostName, configuration.HttpPort))
            {
                var remoteLockGetterProvider = new RemoteLockGetterProvider(httpExternalDataGetter, configuration, externalLogger);
                var test = new SimpleTest(configuration, remoteLockGetterProvider.RemoteLockGetter, externalLogger);
                using (var testRunner = new TestRunner<SimpleProgressMessage>(configuration, externalLogger))
                    testRunner.RunTestAndPublishResults(test);
            }
        }
    }
}