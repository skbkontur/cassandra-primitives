using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

using Microsoft.Win32.TaskScheduler;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.RemoteTaskRunning;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.TestConfigurations;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Processes
{
    public class RemoteProcessLauncher : IProcessLauncher<SimpleTestResult.Merged>
    {
        public RemoteProcessLauncher(ITeamCityLogger teamCityLogger, List<RemoteAgentInfo> agentInfos)
        {
            this.teamCityLogger = teamCityLogger;
            tasks = new List<Task>();
            this.agentInfos = agentInfos;
        }

        private void DeployTask(string targetDirectory)
        {
            var templateDir = new DirectoryInfo(AppDomain.CurrentDomain.BaseDirectory);
            var targetDir = new DirectoryInfo(targetDirectory);
            templateDir.CopyTo(targetDir, true);
        }

        private void CleanDirectory(string directory)
        {
            Directory.Delete(directory, true);
        }

        public void DeployTasks(IEnumerable<RemoteAgent> agents)
        {
            foreach (var agent in agents)
            {
                using (new ImpersonateUser(agent.Credentials))
                {
                    var remoteDir = agent.ProcessDirectory.AsRemote;
                    teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Cleaning directory {1} for process {0}...", agent.ProcessInd, remoteDir);
                    if (Directory.Exists(remoteDir))
                        CleanDirectory(remoteDir);
                    teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Deploying task {0} to '{1}'...", agent.ProcessInd, remoteDir);
                    DeployTask(remoteDir);
                }
            }
        }

        public void StartProcesses(TestConfiguration configuration)
        {
            StopTasks();
            tasks.Clear();
            if (agentInfos.Count < configuration.amountOfProcesses)
                throw new Exception(String.Format("Not enoung agents to run {0} processes", configuration.amountOfProcesses));
            var agents = agentInfos
                .Take(configuration.amountOfProcesses)
                .Select((agent, i) => new RemoteAgent(agent, i))
                .ToList();
            DeployTasks(agents);
            foreach (var agent in agents)
            {
                teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Starting process {0} on agent {1}...", agent.ProcessInd, agent.Name);

                var testRunnerPath = Path.Combine(agent.ProcessDirectory.AsLocal, "Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.exe");
                var wrapperPath = Path.Combine(agent.WorkDirectory.AsRemote, @"JobObjects\MoreJobObjects.exe");
                using (var taskScheduler = new TaskSchedulerAdapter(agent.Credentials, wrapperPath))
                {
                    var task = taskScheduler.RunTaskInWrapper(String.Format("BenchmarkProcess_{0}", agent.ProcessInd), testRunnerPath, new[] {agent.ProcessInd.ToString()}, agent.ProcessDirectory.AsLocal);
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
    }
}