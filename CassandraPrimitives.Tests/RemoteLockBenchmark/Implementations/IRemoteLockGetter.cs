namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Implementations
{
    public interface IRemoteLockGetter
    {
        IRemoteLock Get(string lockId);
    }
}