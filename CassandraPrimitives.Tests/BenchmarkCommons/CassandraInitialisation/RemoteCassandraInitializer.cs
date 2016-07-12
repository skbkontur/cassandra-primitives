using System.Collections.Generic;
using System.IO;

using Microsoft.Win32.TaskScheduler;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.RemoteTaskRunning;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.CassandraInitialisation
{
    public class RemoteCassandraInitializer : ICassandraInitialiser
    {
        public RemoteCassandraInitializer(RemoteTaskSchedulerCredentials credentials, string remoteWorkDir)
        {
            remoteTasks = new List<Task>();
            this.credentials = credentials;
            this.remoteWorkDir = remoteWorkDir;
        }

        public void CreateNode(CassandraNodeSettings settings)
        {
            using (var taskSchedulerAdapter = new TaskSchedulerAdapter(credentials))
            {
                var deployDirectory = Path.Combine(remoteWorkDir, "..", "Cassandra1.2");
                settings.DeployDirectory = deployDirectory;
                CassandraDeployer.DeployCassandra(settings);
                var task = taskSchedulerAdapter.RunTaskInWrapper("CassandraNode", Path.Combine(deployDirectory, "bin", "cassandra.bat"));
                remoteTasks.Add(task);
            }
        }

        public void StopAllNodes()
        {
            foreach (var remoteTask in remoteTasks)
                remoteTask.Stop();
        }

        public void Dispose()
        {
            StopAllNodes();
        }

        private readonly List<Task> remoteTasks;
        private readonly RemoteTaskSchedulerCredentials credentials;
        private readonly string remoteWorkDir;
    }
}