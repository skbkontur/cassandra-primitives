using System;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.TestConfigurations;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Processes
{
    public interface IProcessLauncher<out TTestResult> : IDisposable
        where TTestResult : ITestResult
    {
        void StartProcesses(TestConfiguration configuration);
        TTestResult WaitForResults();
    }
}