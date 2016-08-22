using System.Collections.Generic;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.Agents.Providers
{
    public interface IAgentProvider
    {
        List<RemoteAgentInfo> AcquireAgents(int amount);
        List<string> GetAllAgentNames();
    }
}