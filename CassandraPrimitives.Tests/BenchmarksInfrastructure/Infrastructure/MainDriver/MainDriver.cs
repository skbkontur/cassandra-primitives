using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.Agents.Providers;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.ExternalLogging.HttpLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.Processes;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.TestConfigurations;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.TestProgressProcessors;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.MainDriver
{
    public class MainDriver
    {
        public MainDriver(ITeamCityLogger teamCityLogger, TestConfiguration configuration, ITestProgressProcessor testProgressProcessor, IAgentProvider agentProvider)
        {
            this.teamCityLogger = teamCityLogger;
            this.configuration = configuration;
            this.testProgressProcessor = testProgressProcessor;
            this.agentProvider = agentProvider;
        }

        public void Run(Dictionary<string, object> optionsSet, Dictionary<string, Func<object>> dynamicOptionsSet)
        {
            teamCityLogger.BeginMessageBlock("Results");

            var testAgents = agentProvider.AcquireAgents(configuration.AmountOfProcesses);
            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Agents for tests: {0}", string.Join(", ", testAgents.Select(agent => agent.Name)));
            var wrapperDeployer = new WrapperDeployer(teamCityLogger);
            wrapperDeployer.DeployWrapperToAgents(testAgents);
            var wrapperRelativePath = wrapperDeployer.GetWrapperRelativePath();
            List<string> processDirectories = new List<string>();

            var dirForCurrentArtifacts = new DirectoryInfo(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "CurrentArtifacts"));
            if (dirForCurrentArtifacts.Exists)
                dirForCurrentArtifacts.Delete(true);
            foreach (var indexedDirectory in processDirectories.Select((d, i) => new {Dir = d, Ind = i}))
            {
                try
                {
                    var logsDir = new DirectoryInfo(Path.Combine(indexedDirectory.Dir, "LogsDirectory"));
                    if (logsDir.Exists)
                        logsDir.Delete(true);
                    var metricsDir = new DirectoryInfo(Path.Combine(indexedDirectory.Dir, "MetricsLogs"));
                    if (metricsDir.Exists)
                        metricsDir.Delete(true);
                }
                catch (Exception e)
                {
                    teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Warning, "Exception while cleaning log directories of {0} process:\n{1}", indexedDirectory.Ind, e);
                }
            }

            try
            {
                using (new HttpTestDataProvider(configuration, optionsSet, dynamicOptionsSet))
                using (new HttpExternalLogProcessor(configuration, teamCityLogger, testAgents, testProgressProcessor))
                using (var processLauncher = new RemoteProcessLauncher(teamCityLogger, testAgents, wrapperRelativePath))
                {
                    processLauncher.StartProcesses(configuration);
                    AllProcessesStarted();
                    processDirectories = processLauncher.GetRunningProcessDirectories();
                    processLauncher.WaitForProcessesToFinish();
                }
            }
            catch (Exception e)
            {
                teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Failure, "Exception occured while working with child processes:\n{0}", e);
            }
            finally
            {
                try
                {
                    teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Copying artifacts...");
                    foreach (var indexedDirectory in processDirectories.Select((d, i) => new {Dir = d, Ind = i}))
                    {
                        try
                        {
                            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Copying artifacts of process {0} (from {1})", indexedDirectory.Ind, indexedDirectory.Dir);
                            var dirForProcessArtifacts = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "CurrentArtifacts", string.Format("Process_{0}", indexedDirectory.Ind));
                            var dirForLogArtifacts = Path.Combine(dirForProcessArtifacts, "Logs");
                            var logsDir = new DirectoryInfo(Path.Combine(indexedDirectory.Dir, "LogsDirectory"));
                            if (logsDir.Exists)
                            {
                                teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Copying artifact of process {0} from {1} to {2}", indexedDirectory.Ind, logsDir.FullName, dirForLogArtifacts);
                                logsDir.CopyTo(new DirectoryInfo(dirForLogArtifacts));
                            }
                            var dirForMetricsArtifacts = Path.Combine(dirForProcessArtifacts, "Metrics");
                            var metricsDir = new DirectoryInfo(Path.Combine(indexedDirectory.Dir, "MetricsLogs"));
                            if (metricsDir.Exists)
                            {
                                teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Copying artifact of process {0} from {1} to {2}", indexedDirectory.Ind, metricsDir.FullName, dirForMetricsArtifacts);
                                metricsDir.CopyTo(new DirectoryInfo(dirForMetricsArtifacts));
                            }
                        }
                        catch (Exception e)
                        {
                            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Warning, "Exception while copying artifacts of {0} process:\n{1}", indexedDirectory.Ind, e);
                        }
                    }
                }
                finally
                {
                    teamCityLogger.EndMessageBlock();
                }
            }
        }

        public event Action AllProcessesStarted = () => { };

        private readonly ITeamCityLogger teamCityLogger;
        private readonly IAgentProvider agentProvider;
        private readonly TestConfiguration configuration;
        private readonly ITestProgressProcessor testProgressProcessor;
    }
}