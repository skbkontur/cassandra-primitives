using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.CassandraInitialisation;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.RemoteTaskRunning;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.CassandraRemoteLock
{
    public class CassandraRemoteNodeStartInfo
    {
        public CassandraRemoteNodeStartInfo(RemoteMachineCredentials credentials, CassandraNodeSettings settings, RemoteDirectory remoteWorkDir)
        {
            Credentials = credentials;
            Settings = settings;
            RemoteWorkDir = remoteWorkDir;
        }

        public RemoteMachineCredentials Credentials { get; private set; }
        public CassandraNodeSettings Settings { get; private set; }
        public RemoteDirectory RemoteWorkDir { get; private set; }
    }
}