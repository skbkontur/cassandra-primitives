using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.TestResults;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.ExternalLogging
{
    public interface IExternalLogProcessor<out TTestResult> where TTestResult : ITestResult
    {
        void StartProcessingLog();
        TTestResult GetTestResult();
    }
}