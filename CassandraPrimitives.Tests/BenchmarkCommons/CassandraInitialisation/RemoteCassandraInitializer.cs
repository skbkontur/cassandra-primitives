﻿using System.Collections.Generic;
using System.IO;

using Microsoft.Win32.TaskScheduler;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.RemoteTaskRunning;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.CassandraInitialisation
{
    public class RemoteCassandraInitializer : ICassandraInitialiser
    {
        public RemoteCassandraInitializer(RemoteMachineCredentials credentials, RemoteDirectory remoteWorkDir, bool noDeploy = false)
        {
            remoteTasks = new List<Task>();
            this.credentials = credentials;
            this.remoteWorkDir = remoteWorkDir;
            this.noDeploy = noDeploy;
        }

        public void CreateNode(CassandraNodeSettings settings)
        {
            var wrapperPath = Path.Combine(remoteWorkDir.AsLocal, @"TaskWrapper\Catalogue.DeployTasks.TaskWrapper.exe");
            using (var taskSchedulerAdapter = new TaskSchedulerAdapter(credentials, wrapperPath))
            {
                var deployDirectory = Path.Combine(remoteWorkDir.AsRemote, "..", "Cassandra1.2");
                if (!noDeploy)
                    CassandraDeployer.DeployCassandra(settings, deployDirectory);
                var task = taskSchedulerAdapter.RunTaskInWrapper("CassandraNode", Path.Combine(remoteWorkDir.AsLocal, "..", "Cassandra1.2", "bin", "cassandra.bat"), directory : Path.Combine(remoteWorkDir.AsLocal, "..", "Cassandra1.2", "bin"));
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