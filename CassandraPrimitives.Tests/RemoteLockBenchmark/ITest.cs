namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark
{
    interface ITest<out T>
        where T : ITestResult
    {
        T GetTestResult();
        void Run();
    }
}