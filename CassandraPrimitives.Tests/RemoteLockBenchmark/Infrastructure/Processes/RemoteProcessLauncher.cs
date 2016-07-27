using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

using Microsoft.Win32.TaskScheduler;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.RemoteTaskRunning;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.Agents;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.TestConfigurations;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.Processes
{
    public class RemoteProcessLauncher : IProcessLauncher
    {
        public RemoteProcessLauncher(ITeamCityLogger teamCityLogger, List<RemoteAgentInfo> agentInfos, string wrapperRelativePath, bool noDeploy = false)
        {
            this.teamCityLogger = teamCityLogger;
            tasks = new List<Task>();
            this.agentInfos = agentInfos;
            this.wrapperRelativePath = wrapperRelativePath;
            this.noDeploy = noDeploy;
        }

        private void DeployTask(RemoteAgent agent)
        {
            if (noDeploy)
                return;
            var remoteDir = agent.ProcessDirectory.AsRemote;
            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Cleaning directory {1} for process {0}...", agent.ProcessInd, remoteDir);
            if (Directory.Exists(remoteDir))
                CleanDirectory(remoteDir);
            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Deploying task {0} to '{1}'...", agent.ProcessInd, remoteDir);
            var templateDir = new DirectoryInfo(AppDomain.CurrentDomain.BaseDirectory);
            var targetDir = new DirectoryInfo(remoteDir);
            templateDir.CopyTo(targetDir, true);
        }

        private void CleanDirectory(string directory)
        {
            Directory.Delete(directory, true);
        }

        public void StartProcesses(TestConfiguration configuration)
        {
            StopTasks();
            tasks.Clear();
            if (agentInfos.Count < configuration.AmountOfProcesses)
                throw new Exception(string.Format("Not enoung agents to run {0} processes", configuration.AmountOfProcesses));
            var agents = agentInfos
                .Take(configuration.AmountOfProcesses)
                .Select((agent, i) => new RemoteAgent(agent, i))
                .ToList();
            foreach (var agent in agents)
            {
                var testRunnerPath = Path.Combine(agent.ProcessDirectory.AsLocal, "Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.exe");
                var wrapperPath = Path.Combine(agent.WorkDirectory.AsRemote, wrapperRelativePath);
                using (var taskScheduler = new TaskSchedulerAdapter(agent.Credentials, wrapperPath))
                {
                    var taskName = string.Format("BenchmarkProcess_{0}", agent.ProcessInd);
                    teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Ensuring there is no existing task with same name ({0})...", taskName);
                    taskScheduler.StopAndDeleteTask(taskName);
                    DeployTask(agent);
                    teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Starting process {0} on agent {1}...", agent.ProcessInd, agent.Name);
                    var task = taskScheduler.RunTaskInWrapper(taskName, testRunnerPath, new[] {BenchmarkConfigurator.ConstantBenchmarkToken, agent.ProcessInd.ToString(), configuration.RemoteHostName, agent.Token}, agent.ProcessDirectory.AsLocal);
                    tasks.Add(task);
                }
            }
        }

        public void WaitForProcessesToFinish()
        {
            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Waiting for processes to finish...");
            foreach (var task in tasks)
            {
                while (task.State != TaskState.Ready)
                    Thread.Sleep(processWaitingIntervalMs);
            }
        }

        public void StopTasks()
        {
            foreach (var task in tasks)
            {
                task.Stop();
                task.Dispose();
            }
        }

        public void Dispose()
        {
            StopTasks();
        }

        private const int processWaitingIntervalMs = 1000;

        private readonly ITeamCityLogger teamCityLogger;
        private readonly List<Task> tasks;
        private readonly List<RemoteAgentInfo> agentInfos;
        private readonly bool noDeploy;
        private readonly string wrapperRelativePath;
    }
}