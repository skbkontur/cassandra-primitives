using System;
using System.Collections.Generic;
using System.Linq;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.RemoteTaskRunning;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Agents
{
    public class AgentProviderAllAgents : IAgentProvider
    {
        public List<RemoteAgentInfo> GetAgents(int amount)
        {
            var agents = Enumerable
                .Range(1, 6)
                .Select(i => String.Format("load{0:00}cat.dev.kontur.ru", i))
                .Select(name => new RemoteAgentInfo(
                                    name,
                                    @"Benchmarks\workdir",
                                    new RemoteMachineCredentials(name)))
                .ToList();
            if (amount > agents.Count)
                throw new ArgumentException(String.Format("Can't provide {0} agents, because there're only {1} agents available", amount, agents.Count));
            return agents.Take(amount).ToList();
        }
    }
}