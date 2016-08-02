namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.TestProgressProcessors
{
    public interface ITestProgressProcessor
    {
        string HandleRawProgressMessage(string request, int processInd);
        string HandleRawLogMessage(string request, int processInd);
    }
}