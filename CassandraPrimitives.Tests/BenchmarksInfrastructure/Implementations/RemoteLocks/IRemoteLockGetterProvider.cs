namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Implementations.RemoteLocks
{
    public interface IRemoteLockGetterProvider
    {
        IRemoteLockGetter GetRemoteLockGetter();
    }
}