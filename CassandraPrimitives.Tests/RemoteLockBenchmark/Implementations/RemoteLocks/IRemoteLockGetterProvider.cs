namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Implementations.RemoteLocks
{
    public interface IRemoteLockGetterProvider
    {
        IRemoteLockGetter GetRemoteLockGetter();
    }
}