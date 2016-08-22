using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.ProgressMessages;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.TestOptions;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.Tests
{
    public interface ITest
    {
        void SetUp();
        void DoWorkInSingleThread(int threadInd);
        void TearDown();
    }

    public interface ITest<TProgressMessage, TTestOptions> : ITest
        where TProgressMessage : IProgressMessage
        where TTestOptions : ITestOptions
    {
    }
}