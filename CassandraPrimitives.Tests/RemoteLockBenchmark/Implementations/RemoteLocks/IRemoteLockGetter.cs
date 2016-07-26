namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Implementations.RemoteLocks
{
    public interface IRemoteLockGetter
    {
        IRemoteLock Get(string lockId);
    }
}