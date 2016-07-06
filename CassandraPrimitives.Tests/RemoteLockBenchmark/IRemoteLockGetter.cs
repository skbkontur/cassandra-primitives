using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark
{
    public interface IRemoteLockGetter
    {
        IRemoteLockCreator[] Get(int amount);
    }
}