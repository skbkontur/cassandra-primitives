using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;

using Microsoft.Win32.TaskScheduler;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.RemoteTaskRunning;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.CassandraInitialisation
{
    public class RemoteCassandraInitializer : ICassandraInitialiser
    {
        public RemoteCassandraInitializer(RemoteMachineCredentials credentials, RemoteDirectory remoteWorkDir, string taskWrapperRelativePath, string tasksGroup)
        {
            remoteTasks = new List<Task>();
            this.credentials = credentials;
            this.remoteWorkDir = remoteWorkDir;
            this.taskWrapperRelativePath = taskWrapperRelativePath;
            this.tasksGroup = tasksGroup;
        }

        public void CreateNode(CassandraNodeSettings settings)
        {
            CreateNode(settings, true);
        }

        public void CreateNodeNotWaitingForStart(CassandraNodeSettings settings)
        {
            CreateNode(settings, false);
        }

        public void CreateNode(CassandraNodeSettings settings, bool waitForStart)
        {
            var wrapperPath = Path.Combine(remoteWorkDir.AsLocal, taskWrapperRelativePath);
            using (var taskSchedulerAdapter = new TaskSchedulerAdapter(credentials, tasksGroup))
            {
                var deployDirectory = Path.Combine(remoteWorkDir.AsRemote, "..", "Cassandra2.2");
                taskSchedulerAdapter.StopAndDeleteTask("CassandraNode");
                CassandraDeployer.DeployCassandra(settings, deployDirectory);
                var task = taskSchedulerAdapter.RunTaskInWrapper(wrapperPath, "CassandraNode", Path.Combine(remoteWorkDir.AsLocal, "..", "Cassandra2.2", "bin", "cassandra.bat"), directory : Path.Combine(remoteWorkDir.AsLocal, "..", "Cassandra2.2", "bin"));
                if (waitForStart)
                {
                    if (!WaitForStart(deployDirectory))
                        throw new Exception(string.Format("Failed to start Cassandra node at {0} (address - {1})", deployDirectory, settings.ListenAddress));
                }
                remoteTasks.Add(task);
            }
        }

        private bool WaitForStart(string deployDirectory, int timeoutSeconds = 30)
        {
            Stopwatch stopwatch = Stopwatch.StartNew();
            string path = Path.Combine(deployDirectory, "logs", "system.log");
            while (stopwatch.Elapsed < TimeSpan.FromSeconds(timeoutSeconds))
            {
                if (File.Exists(path))
                {
                    using (FileStream fileStream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                    {
                        using (StreamReader streamReader = new StreamReader((Stream)fileStream))
                        {
                            string str;
                            do
                            {
                                str = streamReader.ReadLine();
                            }
                            while (string.IsNullOrEmpty(str) || !str.Contains("Listening for thrift clients..."));
                            return true;
                        }
                    }
                }
                Thread.Sleep(500);
            }
            return false;
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
        private readonly string taskWrapperRelativePath;
        private readonly string tasksGroup;
    }
}