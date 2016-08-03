using System;
using System.Collections.Generic;
using System.Linq;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.Agents.Providers
{
    public class AgentProviderFromTeamCity : AbstractAgentProvider
    {
        public override List<string> GetAllAgentNames()
        {
            var rawAgents = Environment.GetEnvironmentVariable("benchmark.Agents");
            if (rawAgents == null)
                throw new Exception("No value was given for parameter benchmark.Agents");

            var agentNames = rawAgents.Split('|').ToList();

            return agentNames;
        }
    }
}