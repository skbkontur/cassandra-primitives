using System;
using System.Collections.Generic;
using System.IO;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.RemoteTaskRunning;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.Agents;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.MainDriver
{
    public class WrapperDeployer
    {
        public WrapperDeployer(ITeamCityLogger teamCityLogger, bool noDeploy)
        {
            this.teamCityLogger = teamCityLogger;
            this.noDeploy = noDeploy;
        }

        public void DeployWrapperToAgents(List<RemoteAgentInfo> agents)
        {
            foreach (var agent in agents)
                DeployWrapper(agent.WorkDirectory);
        }

        public void DeployWrapper(RemoteDirectory workDir)
        {
            if (noDeploy)
                return;
            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Deploying wrapper to {0}", workDir.AsRemote);
            var source = new DirectoryInfo(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "..", "..", "..", "..", "Assemblies", "TaskWrapper"));
            var remoteDir = Path.Combine(workDir.AsRemote, "TaskWrapper");
            source.CopyTo(new DirectoryInfo(remoteDir), true);
        }

        private readonly ITeamCityLogger teamCityLogger;
        private readonly bool noDeploy;
    }
}