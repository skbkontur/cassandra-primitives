using System;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.RemoteTaskRunning;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Agents
{
    public class RemoteAgentInfo
    {
        public RemoteAgentInfo(string name, string workDirPathFromDiscC, RemoteMachineCredentials credentials, string token)
        {
            Name = name;
            WorkDirectory = new RemoteDirectory(string.Format(@"\\{0}\c$\", name), @"C:\", workDirPathFromDiscC);
            Credentials = credentials;
            Token = token;
        }

        internal RemoteAgentInfo(string name, RemoteDirectory workDirectory, RemoteMachineCredentials credentials, string token)
        {
            Name = name;
            WorkDirectory = workDirectory;
            Credentials = credentials;
            Token = token;
        }

        public string Name { get; private set; }

        public RemoteDirectory WorkDirectory { get; set; }

        public RemoteMachineCredentials Credentials { get; private set; }

        public string Token { get; set; }
    }
}