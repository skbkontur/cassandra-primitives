using System;
using System.Collections.Generic;
using System.Linq;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.Agents.Providers
{
    public class AgentProviderFromTeamCity : AbstractAgentProvider
    {
        internal override List<string> GetAllAgentNames()
        {
            int amount;
            if (!int.TryParse(Environment.GetEnvironmentVariable("benchmark.AmountOfAgents"), out amount))
                throw new Exception("Invalid value was given for parameter benchmark.AmountOfAgents");

            var agentNames = Enumerable
                .Range(0, amount)
                .Select(i => Environment.GetEnvironmentVariable(string.Format("benchmark.Agent{0}", i)))
                .ToList();
            if (agentNames.Any(string.IsNullOrEmpty))
                throw new Exception(string.Format("Null or empty agent name was given for agent with id {0}", agentNames.FindIndex(string.IsNullOrEmpty)));

            return agentNames;
        }
    }
}