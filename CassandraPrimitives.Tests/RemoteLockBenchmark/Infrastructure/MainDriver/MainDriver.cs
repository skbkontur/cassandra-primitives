using System;
using System.Collections.Generic;
using System.Linq;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.Agents;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.ExternalLogging.HttpLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.Processes;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.TestConfigurations;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.TestProgressProcessors;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.MainDriver
{
    public class MainDriver
    {
        public MainDriver(ITeamCityLogger teamCityLogger, TestConfiguration configuration, IAgentProvider agentProvider, bool noDeploy)
        {
            this.teamCityLogger = teamCityLogger;
            this.noDeploy = noDeploy;
            this.agentProvider = agentProvider;
            this.configuration = configuration;
        }

        public void Run(Dictionary<string, object> optionsSet)
        {
            teamCityLogger.BeginMessageBlock("Results");

            var testAgents = agentProvider.AcquireAgents(configuration.AmountOfProcesses);
            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Agents for tests: {0}", string.Join(", ", testAgents.Select(agent => agent.Name)));
            var wrapperDeployer = new WrapperDeployer(teamCityLogger, noDeploy);
            wrapperDeployer.DeployWrapperToAgents(testAgents);
            var wrapperRelativePath = wrapperDeployer.GetWrapperRelativePath();

            try
            {
                using (new HttpTestDataProvider(configuration, optionsSet))
                using (var testProcessor = new TimelineTestProgressProcessor(configuration, teamCityLogger))
                using (new HttpExternalLogProcessor(configuration, teamCityLogger, testAgents, testProcessor))
                using (var processLauncher = new RemoteProcessLauncher(teamCityLogger, testAgents, wrapperRelativePath, noDeploy))
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
        private readonly bool noDeploy;
        private readonly IAgentProvider agentProvider;
        private readonly TestConfiguration configuration;
    }
}