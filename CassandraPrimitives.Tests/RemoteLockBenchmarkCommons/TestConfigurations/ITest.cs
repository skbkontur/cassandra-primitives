using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.ExternalLogging;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.TestConfigurations
{
    public interface ITest
    {
        void SetUp();
        void DoWorkInSingleThread(int threadInd);
        void TearDown();
    }
}