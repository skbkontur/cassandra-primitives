using System;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.CassandraRemoteLock;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Logging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Processes;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.TestConfigurations;
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
            using (new CassandraClusterStarter())
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
}