using System.Linq;
using System.Net;
using System.Net.Sockets;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.RemoteTaskRunning;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.Agents
{
    public class RemoteAgentInfo
    {
        public RemoteAgentInfo(string name, string workDirPathFromDiscC, RemoteMachineCredentials credentials, string token)
        {
            Name = name;
            WorkDirectory = new RemoteDirectory(string.Format(@"\\{0}\c$\", name), @"C:\", workDirPathFromDiscC);
            Credentials = credentials;
            Token = token;
            IpAddress = Dns.GetHostAddresses(Name).First(x => x.AddressFamily == AddressFamily.InterNetwork);
        }

        internal RemoteAgentInfo(RemoteAgentInfo otherAgentInfo)
        {
            Name = otherAgentInfo.Name;
            WorkDirectory = otherAgentInfo.WorkDirectory;
            Credentials = otherAgentInfo.Credentials;
            Token = otherAgentInfo.Token;
            IpAddress = otherAgentInfo.IpAddress;
        }

        public string Name { get; private set; }

        public RemoteDirectory WorkDirectory { get; private set; }

        public RemoteMachineCredentials Credentials { get; private set; }

        public string Token { get; private set; }
        public IPAddress IpAddress { get; private set; }
    }
}