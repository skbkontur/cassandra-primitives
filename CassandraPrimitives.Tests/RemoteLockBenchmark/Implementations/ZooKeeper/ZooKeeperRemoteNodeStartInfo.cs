using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.RemoteTaskRunning;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.ZookeeperInitialisaton;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Implementations.ZooKeeper
{
    public class ZookeeperRemoteNodeStartInfo
    {
        public ZookeeperRemoteNodeStartInfo(RemoteMachineCredentials credentials, ZookeeperNodeSettings settings, RemoteDirectory remoteWorkDir, string taskWrapperRelativePath)
        {
            Credentials = credentials;
            Settings = settings;
            RemoteWorkDir = remoteWorkDir;
            TaskWrapperPath = RemoteWorkDir.Combine(taskWrapperRelativePath);
        }

        public RemoteMachineCredentials Credentials { get; private set; }
        public ZookeeperNodeSettings Settings { get; private set; }
        public RemoteDirectory RemoteWorkDir { get; private set; }
        public RemoteDirectory TaskWrapperPath { get; private set; }
    }
}