using System;
using System.Collections.Generic;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.TestConfigurations;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.Processes
{
    public interface IProcessLauncher : IDisposable
    {
        void StartProcesses(TestConfiguration configuration);
        void WaitForProcessesToFinish();
        List<string> GetRunningProcessDirectories();
    }
}