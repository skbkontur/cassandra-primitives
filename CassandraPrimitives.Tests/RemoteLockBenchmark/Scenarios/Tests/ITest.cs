using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.ProgressMessages;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.Tests
{
    public interface ITest<TProgressMessage>
        where TProgressMessage : IProgressMessage
    {
        void SetUp();
        void DoWorkInSingleThread(int threadInd);
        void TearDown();
    }
}