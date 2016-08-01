namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.TestProgressProcessors
{
    public interface ITestProgressProcessor
    {
        string HandlePublishProgress(string request, int processInd);
        string HandleLogMessage(string request, int processInd);
    }
}