using System;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.CassandraRemoteLock;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.ExternalLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.TestConfigurations;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark
{
    public class ChildProcessDriver
    {
        public static void RunSingleTest(int processInd, TestConfiguration configuration)
        {
            var externalLogger = new SimpleExternalLogger(Console.Out);
            var settings = new CassandraClusterSettings();
            using (var remoteLockGetter = new CassandraRemoteLockGetter(settings, externalLogger))
            {
                var test = new SimpleTest(configuration, processInd, remoteLockGetter);
                using (var testRunner = new TestRunner<SimpleTestResult>(configuration, externalLogger))
                    testRunner.RunTestAndPublishResults(test);
            }
        }
    }
}