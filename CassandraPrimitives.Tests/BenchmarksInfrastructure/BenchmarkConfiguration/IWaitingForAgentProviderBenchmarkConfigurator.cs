using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.Agents.Providers;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.TestConfigurations;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.BenchmarkConfiguration
{
    public interface IWaitingForAgentProviderBenchmarkConfigurator
    {
        IReadyToStartBenchmarkConfigurator WithAgentProvider(IAgentProvider agentProvider);
        IReadyToStartBenchmarkConfigurator WithAgentProviderFromTeamCity();
        IReadyToStartBenchmarkConfigurator WithAgentProviderFromTeamCity(IEnvironmentVariableProvider variableProvider);
    }
}