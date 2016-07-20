using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.TestConfigurations;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.ExternalLogging
{
    public interface IExternalLogProcessor<out TTestResult> where TTestResult : ITestResult
    {
        void StartProcessingLog();
        TTestResult GetTestResult(int processInd);
    }
}