using System;

using Newtonsoft.Json;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.CassandraRemoteLock;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Logging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Processes;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark
{
    public class Program
    {
        private static void RunMainDriver(TestConfiguration configuration)
        {
            Log4NetConfiguration.InitializeOnce();

            var teamCityLogger = new TeamCityLogger(Console.Out);
            teamCityLogger.BeginMessageBlock("Results");

            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Starting cassandra...");
            using (new CassandraServerStarter())
            {
                try
                {
                    using (var processLauncher = new LocalProcessLauncher(teamCityLogger))
                    {
                        processLauncher.StartProcesses(configuration);

                        var testResult = processLauncher.WaitForResults();

                        teamCityLogger.EndMessageBlock();

                        teamCityLogger.SetBuildStatus(TeamCityBuildStatus.Success, testResult.GetShortMessage());
                    }
                }
                catch (Exception e)
                {
                    teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Failure, "Exception occured while working with child processes:\n{0}", e);
                    teamCityLogger.EndMessageBlock();
                    teamCityLogger.SetBuildStatus("Fail", "Fail because of unexpected exceptions");
                }
            }
        }

        private static void RunSingleTest(int processInd, TestConfiguration configuration)
        {
            SimpleTestResult testResult;
            var settings = new CassandraClusterSettings();
            using (var remoteLockGetter = new CassandraRemoteLockGetter(settings, new FakeExternalLogger()))
            {
                var test = new SimpleTest(configuration, processInd, remoteLockGetter);
                using (var testRunner = new TestRunner(configuration, new FakeExternalLogger()))
                    testResult = testRunner.RunTest(test);
            }
            Console.Write(JsonConvert.SerializeObject(testResult));
        }

        private static void Main(string[] args)
        {
            var configuration = new TestConfiguration
                {
                    amountOfThreads = 5,
                    amountOfProcesses = 3,
                    amountOfLocksPerThread = 40,
                    maxWaitTimeMilliseconds = 100
                };

            if (args.Length == 0)
                RunMainDriver(configuration);
            else
            {
                int threadInd;
                if (!int.TryParse(args[0], out threadInd))
                    Console.WriteLine("Invalid argument");
                RunSingleTest(threadInd, configuration);
            }
        }
    }
}