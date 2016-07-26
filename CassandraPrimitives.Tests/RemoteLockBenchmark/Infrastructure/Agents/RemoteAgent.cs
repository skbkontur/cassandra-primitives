using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.RemoteTaskRunning;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.Agents
{
    public class RemoteAgent : RemoteAgentInfo
    {
        public RemoteAgent(string name, string workDirPathFromDiscC, int processInd, RemoteMachineCredentials credentials, string token)
            : base(name, workDirPathFromDiscC, credentials, token)
        {
            ProcessInd = processInd;
        }

        public RemoteAgent(RemoteAgentInfo agent, int processInd)
            : base(agent)
        {
            ProcessInd = processInd;
        }

        public RemoteDirectory ProcessDirectory { get { return WorkDirectory.Combine("..", string.Format("Process_{0}", ProcessInd)); } }

        public int ProcessInd { get; private set; }
    }
}