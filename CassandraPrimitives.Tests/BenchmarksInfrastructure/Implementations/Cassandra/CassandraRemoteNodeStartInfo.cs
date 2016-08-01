using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.CassandraInitialisation;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.RemoteTaskRunning;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Implementations.Cassandra
{
    public class CassandraRemoteNodeStartInfo
    {
        public CassandraRemoteNodeStartInfo(RemoteMachineCredentials credentials, CassandraNodeSettings settings, RemoteDirectory remoteWorkDir, string taskWrapperRelativePath)
        {
            Credentials = credentials;
            Settings = settings;
            RemoteWorkDir = remoteWorkDir;
            TaskWrapperRelativePath = taskWrapperRelativePath;
        }

        public RemoteMachineCredentials Credentials { get; private set; }
        public CassandraNodeSettings Settings { get; private set; }
        public RemoteDirectory RemoteWorkDir { get; private set; }
        public string TaskWrapperRelativePath { get; private set; }
    }
}