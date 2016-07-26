using System.Collections.Generic;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Agents
{
    public interface IAgentProvider
    {
        List<RemoteAgentInfo> AcquireAgents(int amount);
    }
}