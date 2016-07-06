using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark
{
    interface IRemoteLockGetter
    {
        IRemoteLockCreator[] Get(int amount);
    }
}