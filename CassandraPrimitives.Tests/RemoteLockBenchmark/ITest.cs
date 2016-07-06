namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark
{
    public interface ITest<out T>
        where T : ITestResult
    {
        T GetTestResult();
        void Run();
    }
}