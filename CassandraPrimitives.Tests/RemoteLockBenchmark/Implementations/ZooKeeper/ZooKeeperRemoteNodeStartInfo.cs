using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.RemoteTaskRunning;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Implementations.ZooKeeper
{
    public class ZookeeperRemoteNodeStartInfo
    {
        public ZookeeperRemoteNodeStartInfo(RemoteMachineCredentials credentials, ZookeeperNodeSettings settings, RemoteDirectory remoteWorkDir)
        {
            Credentials = credentials;
            Settings = settings;
            RemoteWorkDir = remoteWorkDir;
        }

        public RemoteMachineCredentials Credentials { get; private set; }
        public ZookeeperNodeSettings Settings { get; private set; }
        public RemoteDirectory RemoteWorkDir { get; private set; }
    }
}