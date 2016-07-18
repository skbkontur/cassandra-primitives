using System;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.RemoteTaskRunning;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark
{
    public class RemoteAgentInfo
    {
        public RemoteAgentInfo(string name, string workDirPathFromDiscC, RemoteMachineCredentials credentials)
        {
            Name = name;
            WorkDirectory = new RemoteDirectory(String.Format(@"\\{0}\c$\", name), @"C:\", workDirPathFromDiscC);
            Credentials = credentials;
        }

        internal RemoteAgentInfo(string name, RemoteDirectory workDirectory, RemoteMachineCredentials credentials)
        {
            Name = name;
            WorkDirectory = workDirectory;
            Credentials = credentials;
        }

        public string Name { get; private set; }

        public RemoteDirectory WorkDirectory { get; set; }

        public RemoteMachineCredentials Credentials { get; private set; }
    }
}