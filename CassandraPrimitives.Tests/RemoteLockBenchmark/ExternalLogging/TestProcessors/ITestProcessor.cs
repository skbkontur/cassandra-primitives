using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.TestConfigurations;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.ExternalLogging.TestProcessors
{
    public interface ITestProcessor
    {
        string HandlePublishProgress(string request, int processInd);
        string HandleLog(string request, int processInd);
        //SimpleTestResult GetTestResult(int processInd);//TODO
    }
}