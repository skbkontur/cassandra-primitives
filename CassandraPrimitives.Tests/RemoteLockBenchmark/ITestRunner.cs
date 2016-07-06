using System;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark
{
    public interface ITestRunner : IDisposable
    {
        T RunTest<T>(ITest<T> test) where T : ITestResult;
    }
}