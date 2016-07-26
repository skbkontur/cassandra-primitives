using System;
using System.Collections.Generic;
using System.Linq;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Agents;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.ExternalLogging.HttpLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.ExternalLogging.TestProgressProcessors;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Processes;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.TestConfigurations;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark
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
            new WrapperDeployer(teamCityLogger, noDeploy).DeployWrapperToAgents(testAgents);

            try
            {
                using (new HttpTestDataProvider(configuration, optionsSet))
                using (var testProcessor = new TimelineTestProgressProcessor(configuration, teamCityLogger))
                using (new HttpExternalLogProcessor(configuration, teamCityLogger, testAgents, testProcessor))
                using (var processLauncher = new RemoteProcessLauncher(teamCityLogger, testAgents, noDeploy))
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