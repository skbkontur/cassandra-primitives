using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.TestResults;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark
{
    public interface ITest<out T>
        where T : ITestResult
    {
        T GetTestResult();
        void SetUp();
        void DoWorkInSingleThread(int threadInd);
        void TearDown();
    }
}