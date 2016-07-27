using System;
using System.Collections.Generic;
using System.Linq;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.RemoteTaskRunning;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.Agents.Providers
{
    public abstract class AbstractAgentProvider : IAgentProvider
    {
        protected AbstractAgentProvider()
        {
            used = 0;
        }

        public List<RemoteAgentInfo> AcquireAgents(int amount)
        {
            if (agents == null)
                CreateAgentInfos();

            if (amount > agents.Count - used)
                throw new ArgumentException(string.Format("Can't provide {0} agents, because there're only {1} agents available", amount, agents.Count - used));
            var result = agents.Skip(used).Take(amount).ToList();
            used += amount;
            return result;
        }

        private void CreateAgentInfos()
        {
            agents = GetAllAgentNames()
                .Select(name => new RemoteAgentInfo(
                                    name,
                                    @"Benchmarks\workdir",
                                    new RemoteMachineCredentials(name),
                                    Guid.NewGuid().ToString()))
                .ToList();
        }

        internal abstract List<string> GetAllAgentNames();

        private int used;
        private List<RemoteAgentInfo> agents;
    }
}