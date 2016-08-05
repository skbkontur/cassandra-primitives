using System.Collections.Generic;
using System.IO;

using Microsoft.Win32.TaskScheduler;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.RemoteTaskRunning;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.ZookeeperInitialisaton
{
    public class RemoteZookeeperInitializer
    {
        public RemoteZookeeperInitializer(RemoteMachineCredentials credentials, RemoteDirectory remoteWorkDir, RemoteDirectory taskWrapperPath, string taskGroup)
        {
            remoteTasks = new List<Task>();
            this.credentials = credentials;
            this.remoteWorkDir = remoteWorkDir;
            this.taskWrapperPath = taskWrapperPath;
            this.taskGroup = taskGroup;
        }

        public void CreateAndStartNode(ZookeeperNodeSettings settings)
        {
            var wrapperPath = taskWrapperPath.AsLocal;

            var deployDirectory = Path.Combine(remoteWorkDir.AsRemote, "..", "ZooKeeper");
            using (var taskSchedulerAdapter = new TaskSchedulerAdapter(credentials, wrapperPath, taskGroup))
            {
                taskSchedulerAdapter.StopAndDeleteTask("ZookeeperNode");
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
        private readonly RemoteDirectory taskWrapperPath;
        private readonly string taskGroup;
    }
}