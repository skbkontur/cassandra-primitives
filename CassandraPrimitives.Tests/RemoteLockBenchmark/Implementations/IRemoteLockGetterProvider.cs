namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Implementations
{
    public interface IRemoteLockGetterProvider
    {
        IRemoteLockGetter GetRemoteLockGetter();
    }
}