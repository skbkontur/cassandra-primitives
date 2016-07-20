using System;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.TestConfigurations;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Processes
{
    public interface IProcessLauncher : IDisposable
    {
        void StartProcesses(TestConfiguration configuration);
        void WaitForProcessesToFinish();
    }
}