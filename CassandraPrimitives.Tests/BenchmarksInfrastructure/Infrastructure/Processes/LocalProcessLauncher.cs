using System;
using System.Diagnostics;
using System.Reflection;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.TestConfigurations;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.Processes
{
    public class LocalProcessLauncher : IProcessLauncher
    {
        public LocalProcessLauncher(ITeamCityLogger teamCityLogger)
        {
            this.teamCityLogger = teamCityLogger;
            workingDirectory = AppDomain.CurrentDomain.BaseDirectory;
            processes = new Process[0];
        }

        public void StartProcesses(TestConfiguration configuration)
        {
            StopProcesses();
            processes = new Process[configuration.AmountOfProcesses];
            for (var i = 0; i < configuration.AmountOfProcesses; i++)
            {
                teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Starting process {0}...", i);

                var startInfo = new ProcessStartInfo
                    {
                        FileName = Assembly.GetExecutingAssembly().Location,
                        Arguments = string.Format("{0}", i),
                        UseShellExecute = false
                    };
                processes[i] = Process.Start(startInfo);
            }
        }

        public void WaitForProcessesToFinish()
        {
            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Waiting for processes to finish...");
            foreach (var process in processes)
                process.WaitForExit();
        }

        public void StopProcesses()
        {
            foreach (var process in processes)
                process.Dispose();
        }

        public void Dispose()
        {
            StopProcesses();
        }

        private Process[] processes;
        private readonly ITeamCityLogger teamCityLogger;
        private readonly string workingDirectory;
    }
}