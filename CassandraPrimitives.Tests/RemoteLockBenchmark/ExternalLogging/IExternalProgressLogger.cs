using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.TestConfigurations;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.ExternalLogging
{
    public interface IExternalProgressLogger<in TTestResult> : IExternalLogger
        where TTestResult : ITestResult
    {
        void PublishResult(TTestResult testResult);
    }
}