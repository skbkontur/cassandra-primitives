namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.TestConfigurations
{
    public interface ITestEnvironment
    {
        string AmountOfThreads { get; }
        string AmountOfProcesses { get; }
        string AmountOfClusterNodes { get; }
        string HttpPort { get; }
        string TestScenario { get; }
        string ClusterType { get; }
    }
}