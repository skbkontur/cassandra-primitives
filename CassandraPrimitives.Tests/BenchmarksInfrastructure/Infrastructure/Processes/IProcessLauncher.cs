using System;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.TestConfigurations;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.Processes
{
    public interface IProcessLauncher : IDisposable
    {
        void StartProcesses(TestConfiguration configuration);
        void WaitForProcessesToFinish();
    }
}