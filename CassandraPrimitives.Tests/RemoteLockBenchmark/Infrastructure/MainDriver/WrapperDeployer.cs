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
            sourceDirectory = new DirectoryInfo(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "..", "..", "..", "..", "Assemblies", "TaskWrapper"));
            sourceHash = sourceDirectory.GetMd5Hash(false);
        }

        public void DeployWrapperToAgents(List<RemoteAgentInfo> agents)
        {
            foreach (var agent in agents)
                DeployWrapper(agent.WorkDirectory);
        }

        public string GetWrapperDirectoryRelativePath()
        {
            return Path.Combine("TaskWrapper", sourceHash);
        }

        public string GetWrapperRelativePath()
        {
            return Path.Combine("TaskWrapper", sourceHash, "Catalogue.DeployTasks.TaskWrapper.exe");
        }

        public void DeployWrapper(RemoteDirectory workDir)
        {
            if (noDeploy)
                return;
            teamCityLogger.BeginMessageBlock("Deploying wrapper");
            try
            {
                teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Source directory hash: {0}", sourceHash);
                var remoteDir = Path.Combine(workDir.AsRemote, GetWrapperDirectoryRelativePath());
                teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Remote directoty for wrapper: {0}", remoteDir);
                var destination = new DirectoryInfo(remoteDir);
                if (!destination.Exists)
                {
                    teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Destination directory {0} doesn't exist, copying from template", destination.FullName);
                    CopyWrapperCheckingHash(sourceDirectory, destination);
                    return;
                }

                teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Directory {0} already exists, checking hash", destination.FullName);
                var destHash = destination.GetMd5Hash(false);
                if (sourceHash == destHash)
                    teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Hashes match, leaving destination folder untouched");
                else
                {
                    teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Warning, "Hashes differ, will try to rewrite destination folder, but probably will get access denied exception");
                    CopyWrapperCheckingHash(sourceDirectory, destination);
                }
            }
            finally
            {
                teamCityLogger.EndMessageBlock();
            }
        }

        private void CopyWrapperCheckingHash(DirectoryInfo source, DirectoryInfo destination)
        {
            source.CopyTo(destination, true);
            var resultHash = destination.GetMd5Hash(false);
            if (resultHash == sourceHash)
                teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Hashes after copying match, success");
            else
                teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Error, "Copying finished, but hashed differ, fail (source hash - {0}, destination hash - {1})", sourceHash, resultHash);
        }

        private readonly ITeamCityLogger teamCityLogger;
        private readonly bool noDeploy;
        private readonly DirectoryInfo sourceDirectory;
        private readonly string sourceHash;
    }
}