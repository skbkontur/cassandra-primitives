using System;
using System.Collections.Generic;
using System.Linq;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.RemoteTaskRunning;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.Agents.Providers
{
    public class AgentProviderWithSingleAgent : IAgentProvider
    {
        public List<RemoteAgentInfo> AcquireAgents(int amount)
        {
            var remote = new RemoteAgentInfo("load01cat.dev.kontur.ru", @"Benchmarks\workdir", new RemoteMachineCredentials("load01cat.dev.kontur.ru", "tc", "load01cat.dev.kontur.ru", "tc_123456"), Guid.NewGuid().ToString());
            var local = new RemoteAgentInfo("K1606012", @"Benchmarks\workdir", new RemoteMachineCredentials("K1606012.kontur"), Guid.NewGuid().ToString());
            return Enumerable.Repeat(remote, amount).ToList();
        }
    }
}