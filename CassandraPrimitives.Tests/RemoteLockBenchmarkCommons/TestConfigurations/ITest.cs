namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.TestConfigurations
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