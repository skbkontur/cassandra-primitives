using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.TestConfigurations;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkChildProcessDriver.ExternalLogging
{
    public interface IExternalProgressLogger<in TTestResult> : IExternalLogger
        where TTestResult : ITestResult
    {
        void PublishResult(TTestResult testResult);
    }
}