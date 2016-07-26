using System;
using System.Collections.Generic;
using System.Linq;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.RemoteTaskRunning;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.Agents
{
    public class AgentProviderAllAgents : IAgentProvider
    {
        public AgentProviderAllAgents()
        {
            used = 0;
            agents = Enumerable
                .Range(1, 6)
                .Select(i => string.Format("load{0:00}cat.dev.kontur.ru", i))
                .Select(name => new RemoteAgentInfo(
                                    name,
                                    @"Benchmarks\workdir",
                                    new RemoteMachineCredentials(name, "tc", name, "tc_123456"),
                                    Guid.NewGuid().ToString()))
                .ToList();
        }

        public List<RemoteAgentInfo> AcquireAgents(int amount)
        {
            if (amount > agents.Count - used)
                throw new ArgumentException(string.Format("Can't provide {0} agents, because there're only {1} agents available", amount, agents.Count - used));
            var result = agents.Skip(used).Take(amount).ToList();
            used += amount;
            return result;
        }

        private int used;
        private readonly List<RemoteAgentInfo> agents;
    }
}