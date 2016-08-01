namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Implementations.RemoteLocks
{
    public interface IRemoteLockGetter
    {
        IRemoteLock Get(string lockId);
    }
}