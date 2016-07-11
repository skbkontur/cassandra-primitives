namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.TestConfigurations
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