using System;
using System.Collections.Generic;
using System.Linq;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.TestConfigurations;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.Agents.Providers
{
    public class AgentProviderFromTeamCity : AbstractAgentProvider
    {
        public AgentProviderFromTeamCity(IEnvironmentVariableProvider variableProvider)
        {
            this.variableProvider = variableProvider;
        }

        public override List<string> GetAllAgentNames()
        {
            var rawAgents = variableProvider.GetValue("Agents");
            if (rawAgents == null)
                throw new Exception("No value was given for parameter Agents");

            var agentNames = rawAgents.Split('|').ToList();

            return agentNames;
        }

        private readonly IEnvironmentVariableProvider variableProvider;
    }
}