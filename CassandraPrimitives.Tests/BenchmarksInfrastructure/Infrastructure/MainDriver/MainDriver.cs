using System;
using System.Collections.Generic;
using System.Linq;

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

        public void Run(Dictionary<string, object> optionsSet)
        {
            teamCityLogger.BeginMessageBlock("Results");

            var testAgents = agentProvider.AcquireAgents(configuration.AmountOfProcesses);
            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Agents for tests: {0}", string.Join(", ", testAgents.Select(agent => agent.Name)));
            var wrapperDeployer = new WrapperDeployer(teamCityLogger);
            wrapperDeployer.DeployWrapperToAgents(testAgents);
            var wrapperRelativePath = wrapperDeployer.GetWrapperRelativePath();

            try
            {
                using (new HttpTestDataProvider(configuration, optionsSet))
                using (new HttpExternalLogProcessor(configuration, teamCityLogger, testAgents, testProgressProcessor))
                using (var processLauncher = new RemoteProcessLauncher(teamCityLogger, testAgents, wrapperRelativePath))
                {
                    processLauncher.StartProcesses(configuration);

                    processLauncher.WaitForProcessesToFinish();

                    teamCityLogger.EndMessageBlock();

                    teamCityLogger.SetBuildStatus(TeamCityBuildStatus.Success, "Done");
                }
            }
            catch (Exception e)
            {
                teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Failure, "Exception occured while working with child processes:\n{0}", e);
                teamCityLogger.EndMessageBlock();
                teamCityLogger.SetBuildStatus("Fail", "Fail because of unexpected exceptions");
            }
        }

        private readonly ITeamCityLogger teamCityLogger;
        private readonly IAgentProvider agentProvider;
        private readonly TestConfiguration configuration;
        private readonly ITestProgressProcessor testProgressProcessor;
    }
}