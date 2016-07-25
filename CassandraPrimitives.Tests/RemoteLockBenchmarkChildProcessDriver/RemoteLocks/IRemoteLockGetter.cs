namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkChildProcessDriver.RemoteLocks
{
    public interface IRemoteLockGetter
    {
        IRemoteLock Get(string lockId);
    }
}