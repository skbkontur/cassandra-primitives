using System;
using System.Collections.Generic;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.TestProgressProcessors
{
    public interface ITestProgressProcessor
    {
        string HandleRawProgressMessage(string request, int processInd);
        string HandleRawLogMessage(string request, int processInd);
        Dictionary<string, Func<object>> GetDynamicOptions();
    }
}