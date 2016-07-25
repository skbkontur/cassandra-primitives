using System.Collections.Generic;
using System.IO;

using Microsoft.Win32.TaskScheduler;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.RemoteTaskRunning;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.ZooKeeper
{
    public class RemoteZookeeperInitializer
    {
        public RemoteZookeeperInitializer(RemoteMachineCredentials credentials, RemoteDirectory remoteWorkDir, bool noDeploy = false)
        {
            remoteTasks = new List<Task>();
            this.credentials = credentials;
            this.remoteWorkDir = remoteWorkDir;
            this.noDeploy = noDeploy;
        }

        public void CreateNode(ZookeeperNodeSettings settings)
        {
            var wrapperPath = Path.Combine(remoteWorkDir.AsLocal, @"TaskWrapper\Catalogue.DeployTasks.TaskWrapper.exe");

            var deployDirectory = Path.Combine(remoteWorkDir.AsRemote, "..", "ZooKeeper");
            if (!noDeploy)
                ZookeeperDeployer.Deploy(settings, deployDirectory);
            using (var taskSchedulerAdapter = new TaskSchedulerAdapter(credentials, wrapperPath))
            {
                var task = taskSchedulerAdapter.RunTaskInWrapper("ZookeeperNode", Path.Combine(remoteWorkDir.AsLocal, "..", "ZooKeeper", "bin", "zkServer.cmd"), directory : Path.Combine(remoteWorkDir.AsLocal, "..", "ZooKeeper", "bin"));
                remoteTasks.Add(task);
            }
        }

        public void StopAllNodes()
        {
            foreach (var remoteTask in remoteTasks)
            {
                remoteTask.Stop();
                remoteTask.Dispose();
            }
        }

        public void Dispose()
        {
            StopAllNodes();
        }

        private readonly List<Task> remoteTasks;
        private readonly RemoteMachineCredentials credentials;
        private readonly RemoteDirectory remoteWorkDir;
        private readonly bool noDeploy;
    }
}