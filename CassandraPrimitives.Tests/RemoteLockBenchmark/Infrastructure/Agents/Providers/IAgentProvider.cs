using System.Collections.Generic;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.Agents.Providers
{
    public interface IAgentProvider
    {
        List<RemoteAgentInfo> AcquireAgents(int amount);
    }
}