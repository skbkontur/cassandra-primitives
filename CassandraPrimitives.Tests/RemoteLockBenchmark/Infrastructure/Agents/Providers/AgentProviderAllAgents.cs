using System.Collections.Generic;
using System.Linq;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.Agents.Providers
{
    public class AgentProviderAllAgents : AbstractAgentProvider
    {
        internal override List<string> GetAllAgentNames()
        {
            return Enumerable
                .Range(1, 6)
                .Select(i => string.Format("load{0:00}cat.dev.kontur.ru", i))
                .ToList();
        }
    }
}