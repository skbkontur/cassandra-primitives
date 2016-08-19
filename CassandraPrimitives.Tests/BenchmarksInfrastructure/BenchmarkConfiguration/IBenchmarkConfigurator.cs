namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.BenchmarkConfiguration
{
    public interface IBenchmarkConfigurator :
        IWaitingForAgentProviderBenchmarkConfigurator,
        IReadyToStartBenchmarkConfigurator
    {
    }
}