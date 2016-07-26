using System;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.TestConfigurations;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.Processes
{
    public interface IProcessLauncher : IDisposable
    {
        void StartProcesses(TestConfiguration configuration);
        void WaitForProcessesToFinish();
    }
}