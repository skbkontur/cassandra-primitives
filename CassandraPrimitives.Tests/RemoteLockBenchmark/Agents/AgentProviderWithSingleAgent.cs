using System.Collections.Generic;
using System.Linq;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.RemoteTaskRunning;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Agents
{
    public class AgentProviderWithSingleAgent : IAgentProvider
    {
        public List<RemoteAgentInfo> GetAgents(int amount)
        {
            var remote = new RemoteAgentInfo("load01cat.dev.kontur.ru", @"Benchmarks\workdir", new RemoteMachineCredentials("load01cat.dev.kontur.ru", "tc", "load01cat.dev.kontur.ru", "tc_123456"));
            var local = new RemoteAgentInfo("K1606012", @"Benchmarks\workdir", new RemoteMachineCredentials("K1606012.kontur"));
            return Enumerable.Repeat(remote, amount).ToList();
        }
    }
}