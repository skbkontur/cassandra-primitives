using System;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.CassandraRemoteLock;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.ExternalLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Logging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Processes;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.TestResults;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark
{
    public class MainDriver
    {
        public static void RunMainDriver(TestConfiguration configuration)
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
    }

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

    public class Program
    {
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
                MainDriver.RunMainDriver(configuration);
            else
            {
                int threadInd;
                if (!int.TryParse(args[0], out threadInd))
                    Console.WriteLine("Invalid argument");
                ChildProcessDriver.RunSingleTest(threadInd, configuration);
            }
        }
    }
}