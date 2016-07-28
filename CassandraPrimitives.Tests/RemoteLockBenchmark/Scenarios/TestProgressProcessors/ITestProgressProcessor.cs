namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.TestProgressProcessors
{
    public interface ITestProgressProcessor
    {
        string HandlePublishProgress(string request, int processInd);
        string HandleLogMessage(string request, int processInd);
    }
}