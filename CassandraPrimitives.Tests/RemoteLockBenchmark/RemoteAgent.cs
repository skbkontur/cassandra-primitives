using System;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.RemoteTaskRunning;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark
{
    public class RemoteAgent : RemoteAgentInfo
    {
        public RemoteAgent(string name, string workDirPathFromDiscC, int processInd, RemoteMachineCredentials credentials)
            : base(name, workDirPathFromDiscC, credentials)
        {
            ProcessInd = processInd;
        }

        public RemoteAgent(RemoteAgentInfo agent, int processInd)
            : base(agent.Name, agent.WorkDirectory, agent.Credentials)
        {
            ProcessInd = processInd;
        }

        public RemoteDirectory ProcessDirectory { get { return WorkDirectory.Combine("..", String.Format("Process_{0}", ProcessInd)); } }

        public int ProcessInd { get; private set; }
    }
}