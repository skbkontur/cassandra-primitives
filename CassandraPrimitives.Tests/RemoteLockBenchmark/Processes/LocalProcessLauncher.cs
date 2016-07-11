using System.Diagnostics;
using System.Linq;
using System.Reflection;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.ExternalLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.TestResults;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Processes
{
    public class LocalProcessLauncher : IProcessLauncher<SimpleTestResult.Merged>
    {
        public LocalProcessLauncher(ITeamCityLogger teamCityLogger)
        {
            this.teamCityLogger = teamCityLogger;
        }

        public void StartProcesses(TestConfiguration configuration)
        {
            processes = new Process[configuration.amountOfProcesses];
            for (var i = 0; i < configuration.amountOfProcesses; i++)
            {
                teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Starting process {0}...", i);

                var startInfo = new ProcessStartInfo
                    {
                        FileName = Assembly.GetExecutingAssembly().Location,
                        Arguments = string.Format("{0}", i),
                        UseShellExecute = false,
                        RedirectStandardOutput = true
                    };
                processes[i] = Process.Start(startInfo);
            }
        }

        public SimpleTestResult.Merged WaitForResults()
        {
            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Waiting for processes to finish...");
            foreach (var process in processes)
                process.WaitForExit();

            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Processing results...");
            var testResults = processes.Select((process, index) =>
                {
                    var logProcessor = new SimpleExternalLogProcessor(process.StandardOutput);
                    logProcessor.StartProcessingLog();
                    var testResult = logProcessor.GetTestResult();
                    teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Process {0} finished with result: {1}", index, testResult.GetShortMessage());
                    return testResult;
                });

            var mergedTestResult = SimpleTestResult.Merge(testResults);
            return mergedTestResult;
        }

        public void Dispose()
        {
            foreach (var process in processes)
                process.Dispose();
        }

        private Process[] processes;
        private readonly ITeamCityLogger teamCityLogger;
    }
}