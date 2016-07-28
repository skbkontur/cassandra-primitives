using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.ProgressMessages;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.Tests
{
    public interface ITest
    {
        void SetUp();
        void DoWorkInSingleThread(int threadInd);
        void TearDown();
    }

    public interface ITest<TProgressMessage> : ITest
        where TProgressMessage : IProgressMessage
    {
    }
}