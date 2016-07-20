using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons
{
    public interface IRemoteLockGetter
    {
        IRemoteLockCreator[] Get(int amount);
    }
}