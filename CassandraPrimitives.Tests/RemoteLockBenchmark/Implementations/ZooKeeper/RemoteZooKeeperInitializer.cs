using System.Collections.Generic;
using System.IO;

using Microsoft.Win32.TaskScheduler;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.RemoteTaskRunning;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Implementations.ZooKeeper
{
    public class RemoteZookeeperInitializer
    {
        public RemoteZookeeperInitializer(RemoteMachineCredentials credentials, RemoteDirectory remoteWorkDir, RemoteDirectory taskWrapperPath, bool noDeploy = false)
        {
            remoteTasks = new List<Task>();
            this.credentials = credentials;
            this.remoteWorkDir = remoteWorkDir;
            this.taskWrapperPath = taskWrapperPath;
            this.noDeploy = noDeploy;
        }

        public void CreateNode(ZookeeperNodeSettings settings)
        {
            var wrapperPath = taskWrapperPath.AsLocal;

            var deployDirectory = Path.Combine(remoteWorkDir.AsRemote, "..", "ZooKeeper");
            using (var taskSchedulerAdapter = new TaskSchedulerAdapter(credentials, wrapperPath))
            {
                taskSchedulerAdapter.StopAndDeleteTask("ZookeeperNode");
                if (!noDeploy)
                    ZookeeperDeployer.Deploy(settings, deployDirectory);
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
        private readonly RemoteDirectory taskWrapperPath;
    }
}