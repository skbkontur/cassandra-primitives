using System;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.ExternalLogging.HttpLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.Registry;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.TestConfigurations;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.TestRunning;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.ChildProcessDriver
{
    public static class ChildProcessDriver
    {
        public static void RunSingleTest(TestConfiguration configuration, int processInd, string processToken, IScenariosRegistry scenariosRegistry)
        {
            using (var externalLogger = new HttpExternalLogger(processInd, configuration.RemoteHostName, processToken))
            {
                try
                {
                    using (var httpExternalDataGetter = new HttpExternalDataGetter(configuration.RemoteHostName, configuration.HttpPort))
                    {
                        var test = scenariosRegistry.CreateTest(configuration.TestScenario, new ScenarioCreationOptions(configuration, processInd, processToken, externalLogger, httpExternalDataGetter));
                        using (var testRunner = new TestRunner(configuration, externalLogger))
                            testRunner.RunTestAndPublishResults(test);
                    }
                }
                catch (Exception e)
                {
                    externalLogger.Log("Unexpected exception: {0}", e);
                }
            }
        }
    }
}